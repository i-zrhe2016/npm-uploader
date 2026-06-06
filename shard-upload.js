import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { execFile } from 'child_process';
import { once } from 'events';
import { finished } from 'stream/promises';
import { promisify } from 'util';

const DEFAULT_PASSWORD = 'your-strong-password-here-123456';
const ALGORITHM = 'aes-256-gcm';
const SALT_SIZE = 16;
const IV_SIZE = 12;
const AUTH_TAG_SIZE = 16;

const CHUNK_SIZE = parsePositiveInt(process.env.SHARD_SIZE_BYTES, 5 * 1024 * 1024);
const PASSWORD = process.env.SHARD_PASSWORD || DEFAULT_PASSWORD;
const FILES_DIR = process.env.FILES_DIR || 'files';
const SHARDS_DIR = process.env.SHARDS_DIR || 'shards';
const UPLOAD_RECORD_FILE =
  process.env.UPLOAD_RECORD_PATH ||
  process.env.UPLOAD_RECORD_FILE ||
  'upload-records.json';
const RECORD_HISTORY_LIMIT = parsePositiveInt(
  process.env.UPLOAD_RECORD_HISTORY_LIMIT,
  20
);
const NPM_SCOPE = normalizeScope(process.env.NPM_SCOPE || '@yourusername');
const NPM_ACCESS = process.env.NPM_ACCESS || 'public';
const PACKAGE_VERSION = process.env.NPM_PACKAGE_VERSION || '1.0.0';
const NPM_REGISTRY = normalizeBaseUrl(
  process.env.NPM_REGISTRY || 'https://registry.npmjs.org'
);
const NPM_WEB_BASE = normalizeBaseUrl(
  process.env.NPM_WEB_BASE || 'https://www.npmjs.com/package'
);
const NPM_TAG = process.env.NPM_TAG || 'latest';
const NPM_PUBLISH_OTP = process.env.NPM_PUBLISH_OTP || '';
const PUBLISH_CONCURRENCY = parsePositiveInt(process.env.PUBLISH_CONCURRENCY, 2);
const DRY_RUN = process.env.DRY_RUN === '1';
const PUBLISH_TIMEOUT_MS = parsePositiveInt(
  process.env.NPM_PUBLISH_TIMEOUT_MS,
  10 * 60 * 1000
);

const execFileAsync = promisify(execFile);

function parsePositiveInt(value, fallback) {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeScope(scope) {
  if (!scope) return '';
  return scope.startsWith('@') ? scope : `@${scope}`;
}

function normalizeBaseUrl(value) {
  return value.replace(/\/+$/, '');
}

function sanitizeName(value) {
  const sanitized = value
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/-{2,}/g, '-');

  return sanitized || 'file';
}

function buildPackageName(scope, artifactName, chunkIndex) {
  const baseName = `${artifactName}-shard-${chunkIndex}`;
  return scope ? `${scope}/${baseName}` : baseName;
}

function buildRegistryMetadataUrl(registry, packageName) {
  return `${registry}/${encodeURIComponent(packageName)}`;
}

function buildPackageWebUrl(webBase, packageName, version) {
  return `${webBase}/${packageName}/v/${version}`;
}

function formatSize(bytes) {
  return `${(bytes / 1024 / 1024).toFixed(2)}MB`;
}

async function writeJson(targetPath, data) {
  await fsp.mkdir(path.dirname(targetPath), { recursive: true });
  await fsp.writeFile(targetPath, JSON.stringify(data, null, 2));
}

async function pathExists(targetPath) {
  try {
    await fsp.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

async function writeBuffer(stream, chunk) {
  if (!stream.write(chunk)) {
    await once(stream, 'drain');
  }
}

async function closeStream(stream) {
  if (!stream || stream.destroyed) return;
  stream.end();
  await finished(stream);
}

async function encryptAndSplitFile(filePath, artifactName) {
  const shardRoot = path.join(SHARDS_DIR, artifactName);
  const shardFileName = `${artifactName}.part`;
  const sourceStats = await fsp.stat(filePath);
  const uploadId = crypto.randomUUID();
  const chunks = [];

  await fsp.rm(shardRoot, { recursive: true, force: true });
  await fsp.mkdir(shardRoot, { recursive: true });

  const salt = crypto.randomBytes(SALT_SIZE);
  const iv = crypto.randomBytes(IV_SIZE);
  const key = crypto.scryptSync(PASSWORD, salt, 32);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv, {
    authTagLength: AUTH_TAG_SIZE
  });
  const input = fs.createReadStream(filePath);
  const encryptedStream = input.pipe(cipher);

  const shardPaths = [];
  let currentStream = null;
  let currentShardSize = 0;
  let currentShardPath = '';
  let shardIndex = 0;
  let encryptedBytes = 0;

  async function openShard() {
    currentShardPath = path.join(shardRoot, `${shardFileName}${shardIndex}`);
    currentStream = fs.createWriteStream(currentShardPath);
    currentShardSize = 0;
    shardPaths.push(currentShardPath);
  }

  async function finalizeShard() {
    if (!currentStream) return;
    await closeStream(currentStream);
    chunks.push({
      index: shardIndex,
      fileName: path.basename(currentShardPath),
      size: currentShardSize
    });
    console.log(
      `✓ 生成切片: ${currentShardPath} (${formatSize(currentShardSize)})`
    );
    currentStream = null;
    currentShardPath = '';
    currentShardSize = 0;
    shardIndex += 1;
  }

  try {
    for await (const chunk of encryptedStream) {
      let offset = 0;

      while (offset < chunk.length) {
        if (!currentStream) {
          await openShard();
        }

        const writableBytes = Math.min(
          CHUNK_SIZE - currentShardSize,
          chunk.length - offset
        );
        const slice = chunk.subarray(offset, offset + writableBytes);

        await writeBuffer(currentStream, slice);

        currentShardSize += writableBytes;
        encryptedBytes += writableBytes;
        offset += writableBytes;

        if (currentShardSize === CHUNK_SIZE) {
          await finalizeShard();
        }
      }
    }

    if (!shardPaths.length) {
      await openShard();
    }

    await finalizeShard();
  } catch (error) {
    input.destroy(error);
    cipher.destroy(error);
    if (currentStream && !currentStream.destroyed) {
      currentStream.destroy(error);
    }
    throw error;
  }

  const manifest = {
    version: 1,
    uploadId,
    artifactName,
    encrypted: true,
    algorithm: ALGORITHM,
    authTagLength: AUTH_TAG_SIZE,
    chunkSize: CHUNK_SIZE,
    totalChunks: shardPaths.length,
    encryptedBytes,
    originalFileName: path.basename(filePath),
    originalFileSize: sourceStats.size,
    packageVersion: PACKAGE_VERSION,
    registry: NPM_REGISTRY,
    access: NPM_ACCESS,
    salt: salt.toString('hex'),
    iv: iv.toString('hex'),
    authTag: cipher.getAuthTag().toString('hex'),
    generatedAt: new Date().toISOString(),
    chunks
  };

  const manifestPath = path.join(shardRoot, 'manifest.json');
  await writeJson(manifestPath, manifest);

  return {
    manifest,
    manifestPath,
    shardRoot,
    shardFileName
  };
}

async function createNpmPackage(
  shardRoot,
  manifestPath,
  artifactName,
  chunkIndex,
  totalChunks,
  packageVersion
) {
  const shardFileName = `${artifactName}.part${chunkIndex}`;
  const pkgDir = path.join(shardRoot, `shard-${chunkIndex}`);
  const packageName = buildPackageName(NPM_SCOPE, artifactName, chunkIndex);

  await fsp.mkdir(pkgDir, { recursive: true });

  const pkgJson = {
    name: packageName,
    version: packageVersion,
    description: `Encrypted shard ${chunkIndex} of ${artifactName}`,
    main: 'index.js',
    files: ['index.js', 'manifest.json', shardFileName],
    private: false,
    license: 'MIT'
  };

  const indexSource = `module.exports = {
  shardIndex: ${chunkIndex},
  totalShards: ${totalChunks},
  manifest: require('./manifest.json'),
  shardFile: '${shardFileName}'
};
`;

  await Promise.all([
    fsp.writeFile(
      path.join(pkgDir, 'package.json'),
      JSON.stringify(pkgJson, null, 2)
    ),
    fsp.writeFile(path.join(pkgDir, 'index.js'), indexSource),
    fsp.copyFile(manifestPath, path.join(pkgDir, 'manifest.json')),
    fsp.copyFile(
      path.join(shardRoot, shardFileName),
      path.join(pkgDir, shardFileName)
    )
  ]);

  return {
    chunkIndex,
    packageName,
    pkgDir,
    version: packageVersion
  };
}

async function publishPackage(chunkIndex, packageName, version, pkgDir) {
  const upload = {
    status: DRY_RUN ? 'dry-run' : 'published',
    packageName,
    version,
    installSpec: `${packageName}@${version}`,
    tag: NPM_TAG,
    access: NPM_ACCESS,
    registry: NPM_REGISTRY,
    packageUrl: buildPackageWebUrl(NPM_WEB_BASE, packageName, version),
    registryMetadataUrl: buildRegistryMetadataUrl(NPM_REGISTRY, packageName),
    publishedAt: DRY_RUN ? null : new Date().toISOString()
  };

  if (DRY_RUN) {
    console.log(`↷ dry-run: 跳过发布 ${packageName}`);
    return {
      chunkIndex,
      upload
    };
  }

  console.log(`正在发布 ${packageName}...`);

  try {
    const publishArgs = ['publish', '--access', NPM_ACCESS, '--tag', NPM_TAG];
    if (NPM_PUBLISH_OTP) {
      publishArgs.push('--otp', NPM_PUBLISH_OTP);
    }

    const { stdout, stderr } = await execFileAsync(
      'npm',
      publishArgs,
      {
        cwd: pkgDir,
        timeout: PUBLISH_TIMEOUT_MS,
        maxBuffer: 10 * 1024 * 1024
      }
    );

    if (stdout.trim()) {
      console.log(stdout.trim());
    }
    if (stderr.trim()) {
      console.error(stderr.trim());
    }

    console.log(`✅ 发布成功: ${packageName}`);
    return {
      chunkIndex,
      upload
    };
  } catch (error) {
    const detail = [
      error.stderr && error.stderr.toString().trim(),
      error.stdout && error.stdout.toString().trim(),
      error.message
    ]
      .filter(Boolean)
      .join('\n');

    throw new Error(`发布失败 ${packageName}\n${detail}`);
  }
}

async function updateUploadRecord(filePath, manifest, manifestPath) {
  const recordPath = path.resolve(UPLOAD_RECORD_FILE);
  const sourceFilePath = path.resolve(filePath);
  const localManifestPath = path.resolve(manifestPath);
  const recordUpdatedAt = new Date().toISOString();
  const uploadId =
    manifest.uploadId ||
    `${manifest.artifactName}:${manifest.packageVersion}:${recordUpdatedAt}`;

  let record = {
    version: 1,
    updatedAt: null,
    artifacts: {}
  };

  if (await pathExists(recordPath)) {
    const parsed = JSON.parse(await fsp.readFile(recordPath, 'utf8'));
    record = {
      version: parsed.version || 1,
      updatedAt: parsed.updatedAt || null,
      artifacts: parsed.artifacts && typeof parsed.artifacts === 'object'
        ? parsed.artifacts
        : {}
    };
  }

  const snapshot = {
    ...cloneJson(manifest),
    uploadId,
    sourceFilePath,
    localManifestPath,
    recordFile: recordPath,
    recordUpdatedAt
  };
  const artifactRecord = record.artifacts[manifest.artifactName] || {
    artifactName: manifest.artifactName,
    latest: null,
    history: []
  };
  const history = Array.isArray(artifactRecord.history)
    ? artifactRecord.history.filter((entry) => entry?.uploadId !== uploadId)
    : [];
  if (
    artifactRecord.latest &&
    artifactRecord.latest.uploadId &&
    artifactRecord.latest.uploadId !== uploadId
  ) {
    history.unshift(artifactRecord.latest);
  }

  record.artifacts[manifest.artifactName] = {
    artifactName: manifest.artifactName,
    latestUploadId: uploadId,
    updatedAt: recordUpdatedAt,
    latest: snapshot,
    history: history.slice(-RECORD_HISTORY_LIMIT)
  };
  record.updatedAt = recordUpdatedAt;

  await writeJson(recordPath, record);

  return {
    recordPath,
    recordUpdatedAt,
    uploadId,
    historyLength: Math.min(history.length, RECORD_HISTORY_LIMIT)
  };
}

async function mapWithConcurrency(items, concurrency, worker) {
  if (!items.length) return [];

  const results = new Array(items.length);
  const failures = [];
  let cursor = 0;

  async function runWorker() {
    while (true) {
      const index = cursor;
      cursor += 1;

      if (index >= items.length) {
        return;
      }

      try {
        results[index] = await worker(items[index], index);
      } catch (error) {
        failures.push(error);
      }
    }
  }

  const workerCount = Math.min(concurrency, items.length);
  await Promise.all(Array.from({ length: workerCount }, runWorker));

  if (failures.length) {
    throw new Error(failures.map((error) => error.message).join('\n\n'));
  }

  return results;
}

async function processFile(filePath) {
  const parsed = path.parse(filePath);
  const artifactName = sanitizeName(parsed.name);

  console.log(`加密并切分文件: ${filePath}`);
  const { manifest, manifestPath, shardRoot } = await encryptAndSplitFile(
    filePath,
    artifactName
  );

  const packageInfos = await Promise.all(
    Array.from({ length: manifest.totalChunks }, (_, chunkIndex) =>
      createNpmPackage(
        shardRoot,
        manifestPath,
        artifactName,
        chunkIndex,
        manifest.totalChunks,
        manifest.packageVersion
      )
    )
  );

  const publishResults = await mapWithConcurrency(
    packageInfos,
    PUBLISH_CONCURRENCY,
    ({ chunkIndex, packageName, pkgDir, version }) =>
      publishPackage(chunkIndex, packageName, version, pkgDir)
  );

  for (const result of publishResults) {
    if (!result) continue;
    manifest.chunks[result.chunkIndex] = {
      ...manifest.chunks[result.chunkIndex],
      upload: result.upload
    };
  }

  manifest.lastUpdatedAt = new Date().toISOString();
  const recordInfo = await updateUploadRecord(filePath, manifest, manifestPath);
  manifest.record = {
    filePath: recordInfo.recordPath,
    latestUploadId: recordInfo.uploadId,
    updatedAt: recordInfo.recordUpdatedAt,
    historyLength: recordInfo.historyLength
  };
  await writeJson(manifestPath, manifest);
  console.log(`已写入上传记录: ${manifestPath}`);
  console.log(`已更新唯一记录文件: ${recordInfo.recordPath}`);
}

async function main() {
  if (PASSWORD === DEFAULT_PASSWORD) {
    console.warn(
      '警告: 当前仍在使用默认密码，建议设置 SHARD_PASSWORD 环境变量后再执行。'
    );
  }

  await fsp.mkdir(FILES_DIR, { recursive: true });

  const entries = await fsp.readdir(FILES_DIR, { withFileTypes: true });
  const files = entries
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name);

  if (!files.length) {
    console.log(`未在 ${FILES_DIR} 目录中找到待处理文件。`);
    return;
  }

  for (const file of files) {
    await processFile(path.join(FILES_DIR, file));
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
