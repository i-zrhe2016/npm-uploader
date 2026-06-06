import fs from 'fs';
import fsp from 'fs/promises';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import { execFile } from 'child_process';
import { once } from 'events';
import { finished, pipeline } from 'stream/promises';
import { promisify } from 'util';

const DEFAULT_PASSWORD = 'your-strong-password-here-123456';
const DEFAULT_CHUNK_VERSION = 'latest';
const DEFAULT_OUTPUT_DIR = 'restored';
const DEFAULT_DOWNLOAD_CONCURRENCY = 4;
const DEFAULT_TIMEOUT_MS = 10 * 60 * 1000;

const execFileAsync = promisify(execFile);
const args = parseArgs(process.argv.slice(2));

const PASSWORD = args.password || process.env.SHARD_PASSWORD || DEFAULT_PASSWORD;
const MANIFEST_PATH = args.manifest || process.env.MANIFEST_PATH || '';
const RECORD_PATH =
  args.record ||
  process.env.UPLOAD_RECORD_PATH ||
  process.env.UPLOAD_RECORD_FILE ||
  'upload-records.json';
const ARTIFACT_NAME = sanitizeName(
  args.artifact || process.env.ARTIFACT_NAME || args.positionals[0] || ''
);
const SOURCE_DIR = args.sourceDir || process.env.SOURCE_DIR || '';
const OUTPUT_DIR = args.outputDir || process.env.OUTPUT_DIR || DEFAULT_OUTPUT_DIR;
const NPM_SCOPE = normalizeScope(args.scope || process.env.NPM_SCOPE || '@yourusername');
const SHARD_VERSION = args.version || process.env.SHARD_VERSION || DEFAULT_CHUNK_VERSION;
const DOWNLOAD_CONCURRENCY = parsePositiveInt(
  args.downloadConcurrency || process.env.DOWNLOAD_CONCURRENCY,
  DEFAULT_DOWNLOAD_CONCURRENCY
);
const REGISTRY = args.registry || process.env.NPM_REGISTRY || '';
const KEEP_TEMP = args.keepTemp || process.env.KEEP_TEMP === '1';
const KEEP_ENCRYPTED = args.keepEncrypted || process.env.KEEP_ENCRYPTED === '1';
const SKIP_DECRYPT = args.skipDecrypt || process.env.SKIP_DECRYPT === '1';
const COMMAND_TIMEOUT_MS = parsePositiveInt(
  args.timeout || process.env.NPM_COMMAND_TIMEOUT_MS,
  DEFAULT_TIMEOUT_MS
);

function parseArgs(argv) {
  const result = {
    positionals: []
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];

    if (!token.startsWith('--')) {
      result.positionals.push(token);
      continue;
    }

    const [flag, inlineValue] = token.split('=', 2);
    const nextValue = inlineValue ?? argv[index + 1];
    const consumeNext = inlineValue == null;

    switch (flag) {
      case '--artifact':
        result.artifact = nextValue;
        if (consumeNext) index += 1;
        break;
      case '--manifest':
        result.manifest = nextValue;
        if (consumeNext) index += 1;
        break;
      case '--record':
        result.record = nextValue;
        if (consumeNext) index += 1;
        break;
      case '--scope':
        result.scope = nextValue;
        if (consumeNext) index += 1;
        break;
      case '--version':
        result.version = nextValue;
        if (consumeNext) index += 1;
        break;
      case '--source-dir':
        result.sourceDir = nextValue;
        if (consumeNext) index += 1;
        break;
      case '--output-dir':
        result.outputDir = nextValue;
        if (consumeNext) index += 1;
        break;
      case '--password':
        result.password = nextValue;
        if (consumeNext) index += 1;
        break;
      case '--download-concurrency':
        result.downloadConcurrency = nextValue;
        if (consumeNext) index += 1;
        break;
      case '--registry':
        result.registry = nextValue;
        if (consumeNext) index += 1;
        break;
      case '--timeout':
        result.timeout = nextValue;
        if (consumeNext) index += 1;
        break;
      case '--keep-temp':
        result.keepTemp = true;
        break;
      case '--keep-encrypted':
        result.keepEncrypted = true;
        break;
      case '--skip-decrypt':
        result.skipDecrypt = true;
        break;
      default:
        throw new Error(`未知参数: ${flag}`);
    }
  }

  return result;
}

function parsePositiveInt(value, fallback) {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeScope(scope) {
  if (!scope) return '';
  return scope.startsWith('@') ? scope : `@${scope}`;
}

function sanitizeName(value) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/-{2,}/g, '-');
}

function buildPackageName(scope, artifactName, chunkIndex) {
  const baseName = `${artifactName}-shard-${chunkIndex}`;
  return scope ? `${scope}/${baseName}` : baseName;
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

async function pathExists(targetPath) {
  try {
    await fsp.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function runCommand(command, commandArgs, options = {}) {
  const execOptions = {
    timeout: COMMAND_TIMEOUT_MS,
    maxBuffer: 10 * 1024 * 1024,
    ...options
  };

  const { stdout, stderr } = await execFileAsync(command, commandArgs, execOptions);

  return {
    stdout: stdout.trim(),
    stderr: stderr.trim()
  };
}

async function readJson(targetPath) {
  const content = await fsp.readFile(targetPath, 'utf8');
  return JSON.parse(content);
}

async function extractTarball(tarballPath, destinationDir) {
  await fsp.mkdir(destinationDir, { recursive: true });
  await runCommand('tar', ['-xzf', tarballPath, '-C', destinationDir]);
}

async function downloadPackage(packageName, version, targetDir, registry) {
  await fsp.rm(targetDir, { recursive: true, force: true });
  await fsp.mkdir(targetDir, { recursive: true });

  const packArgs = ['pack', `${packageName}@${version}`, '--silent'];
  if (registry) {
    packArgs.push('--registry', registry);
  }

  const { stdout } = await runCommand('npm', packArgs, { cwd: targetDir });
  const tarballName = stdout.split('\n').filter(Boolean).pop();

  if (!tarballName) {
    throw new Error(`未能获取 ${packageName} 的 tarball 名称。`);
  }

  const tarballPath = path.join(targetDir, tarballName);
  const unpackDir = path.join(targetDir, 'unpacked');
  await extractTarball(tarballPath, unpackDir);

  const packageDir = path.join(unpackDir, 'package');
  if (!(await pathExists(packageDir))) {
    throw new Error(`解压 ${packageName} 后未找到 package 目录。`);
  }

  return packageDir;
}

async function readManifest(packageDir) {
  const manifestPath = path.join(packageDir, 'manifest.json');
  return readJson(manifestPath);
}

function resolveManifestDocument(document, cliArtifactName) {
  if (!document) {
    return {
      manifest: null,
      artifactName: cliArtifactName
    };
  }

  if (document.artifacts && !document.chunks) {
    const artifactNames = Object.keys(document.artifacts);
    const artifactName =
      cliArtifactName || (artifactNames.length === 1 ? artifactNames[0] : '');

    if (!artifactName) {
      throw new Error(
        '记录文件中包含多个 artifact，请通过 --artifact 或 ARTIFACT_NAME 指定要恢复的项目。'
      );
    }

    const latest = document.artifacts[artifactName]?.latest;
    if (!latest) {
      throw new Error(`记录文件中未找到 artifact: ${artifactName}`);
    }

    return {
      manifest: latest,
      artifactName
    };
  }

  return {
    manifest: document,
    artifactName: resolveArtifactName(cliArtifactName, document)
  };
}

function resolveArtifactName(cliArtifactName, manifest) {
  if (manifest?.artifactName) {
    return manifest.artifactName;
  }

  if (cliArtifactName) {
    return cliArtifactName;
  }

  if (manifest?.originalFileName) {
    return sanitizeName(path.parse(manifest.originalFileName).name);
  }

  return '';
}

function getChunkFileName(manifest, artifactName, chunkIndex) {
  return manifest?.chunks?.[chunkIndex]?.fileName || `${artifactName}.part${chunkIndex}`;
}

function getChunkPackageName(manifest, artifactName, chunkIndex) {
  return (
    manifest?.chunks?.[chunkIndex]?.upload?.packageName ||
    buildPackageName(NPM_SCOPE, artifactName, chunkIndex)
  );
}

function getChunkVersion(manifest, chunkIndex) {
  return (
    manifest?.chunks?.[chunkIndex]?.upload?.version ||
    manifest?.packageVersion ||
    SHARD_VERSION
  );
}

function getChunkRegistry(manifest) {
  return manifest?.chunks?.[0]?.upload?.registry || manifest?.registry || REGISTRY;
}

async function resolveLocalPackageDir(sourceDir, chunkIndex) {
  const candidate = path.join(sourceDir, `shard-${chunkIndex}`);
  if (await pathExists(candidate)) {
    return candidate;
  }

  throw new Error(`本地来源缺少目录: ${candidate}`);
}

async function mapWithConcurrency(items, concurrency, worker) {
  if (!items.length) return [];

  const results = new Array(items.length);
  const failures = [];
  let cursor = 0;

  async function runWorker() {
    while (true) {
      const currentIndex = cursor;
      cursor += 1;

      if (currentIndex >= items.length) {
        return;
      }

      try {
        results[currentIndex] = await worker(items[currentIndex], currentIndex);
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

async function collectPackages(artifactName, tempRoot, manifestOverride) {
  if (!artifactName) {
    throw new Error(
      '缺少分片标识。请传入 `node shard-restore.js <artifactName>`、设置 ARTIFACT_NAME，或提供包含 artifactName 的 manifest。'
    );
  }

  if (!SOURCE_DIR && !NPM_SCOPE) {
    throw new Error('未提供 SOURCE_DIR，且 NPM_SCOPE 为空，无法从 npm 下载分片。');
  }

  if (SOURCE_DIR) {
    const firstPackageDir = await resolveLocalPackageDir(SOURCE_DIR, 0);
    const manifest = manifestOverride || (await readManifest(firstPackageDir));
    const packageDirs = await Promise.all(
      Array.from({ length: manifest.totalChunks }, (_, chunkIndex) =>
        resolveLocalPackageDir(SOURCE_DIR, chunkIndex)
      )
    );

    return {
      manifest,
      packageDirs,
      tempRoot: ''
    };
  }

  const downloadRoot = await fsp.mkdtemp(path.join(tempRoot, `${artifactName}-`));
  const firstManifestPackageName = getChunkPackageName(
    manifestOverride,
    artifactName,
    0
  );
  const firstPackageVersion = getChunkVersion(manifestOverride, 0);
  const registry = getChunkRegistry(manifestOverride);

  console.log(`下载分片包: ${firstManifestPackageName}@${firstPackageVersion}`);
  const firstPackageDir = await downloadPackage(
    firstManifestPackageName,
    firstPackageVersion,
    path.join(downloadRoot, 'shard-0'),
    registry
  );
  const manifest = manifestOverride || (await readManifest(firstPackageDir));
  const packageDirs = new Array(manifest.totalChunks);
  packageDirs[0] = firstPackageDir;

  const remainingIndexes = Array.from(
    { length: manifest.totalChunks - 1 },
    (_, offset) => offset + 1
  );

  await mapWithConcurrency(
    remainingIndexes,
    DOWNLOAD_CONCURRENCY,
    async (chunkIndex) => {
      const packageName = getChunkPackageName(manifest, artifactName, chunkIndex);
      const version = getChunkVersion(manifest, chunkIndex);
      console.log(`下载分片包: ${packageName}@${version}`);
      const packageDir = await downloadPackage(
        packageName,
        version,
        path.join(downloadRoot, `shard-${chunkIndex}`),
        getChunkRegistry(manifest)
      );
      packageDirs[chunkIndex] = packageDir;
    }
  );

  return {
    manifest,
    packageDirs,
    tempRoot: downloadRoot
  };
}

async function mergeEncryptedFile(packageDirs, manifest, artifactName, outputDir) {
  await fsp.mkdir(outputDir, { recursive: true });

  const encryptedPath = path.join(outputDir, `${artifactName}.enc`);
  const output = fs.createWriteStream(encryptedPath);
  let mergedBytes = 0;

  try {
    for (let chunkIndex = 0; chunkIndex < manifest.totalChunks; chunkIndex += 1) {
      const partFile = path.join(
        packageDirs[chunkIndex],
        getChunkFileName(manifest, artifactName, chunkIndex)
      );
      if (!(await pathExists(partFile))) {
        throw new Error(`缺少分片文件: ${partFile}`);
      }

      console.log(`合并分片: ${path.basename(partFile)}`);

      const input = fs.createReadStream(partFile);
      for await (const chunk of input) {
        mergedBytes += chunk.length;
        await writeBuffer(output, chunk);
      }
    }

    await closeStream(output);
  } catch (error) {
    output.destroy(error);
    throw error;
  }

  if (mergedBytes !== manifest.encryptedBytes) {
    throw new Error(
      `合并后的加密文件大小不匹配，期望 ${manifest.encryptedBytes}，实际 ${mergedBytes}`
    );
  }

  return encryptedPath;
}

async function decryptFile(encryptedPath, manifest, outputDir) {
  const salt = Buffer.from(manifest.salt, 'hex');
  const iv = Buffer.from(manifest.iv, 'hex');
  const authTag = Buffer.from(manifest.authTag, 'hex');
  const key = crypto.scryptSync(PASSWORD, salt, 32);
  const decipher = crypto.createDecipheriv(manifest.algorithm, key, iv, {
    authTagLength: manifest.authTagLength
  });
  decipher.setAuthTag(authTag);

  const restoredPath = path.join(outputDir, manifest.originalFileName);
  await pipeline(
    fs.createReadStream(encryptedPath),
    decipher,
    fs.createWriteStream(restoredPath)
  );

  return restoredPath;
}

async function cleanupTemp(targetPath) {
  if (!targetPath || KEEP_TEMP) return;
  await fsp.rm(targetPath, { recursive: true, force: true });
}

async function main() {
  if (PASSWORD === DEFAULT_PASSWORD) {
    console.warn(
      '警告: 当前仍在使用默认密码，建议设置 SHARD_PASSWORD 环境变量后再恢复。'
    );
  }

  const hasDefaultRecord = !MANIFEST_PATH && (await pathExists(RECORD_PATH));

  if (!SOURCE_DIR && !MANIFEST_PATH && !hasDefaultRecord && NPM_SCOPE === '@yourusername') {
    console.warn(
      '警告: 当前仍在使用默认 NPM_SCOPE，建议设置真实 scope 后再从 npm 拉取分片。'
    );
  }

  const tempBase = await fsp.mkdtemp(path.join(os.tmpdir(), 'shard-restore-'));

  try {
    const documentPath = MANIFEST_PATH
      ? MANIFEST_PATH
      : hasDefaultRecord
        ? RECORD_PATH
        : '';
    const document = documentPath ? await readJson(documentPath) : null;
    const resolved = resolveManifestDocument(document, ARTIFACT_NAME);
    const manifestOverride = resolved.manifest;
    const artifactName = resolved.artifactName;
    const { manifest, packageDirs, tempRoot } = await collectPackages(
      artifactName,
      tempBase,
      manifestOverride
    );
    const restoredOutputDir = path.resolve(OUTPUT_DIR, artifactName);

    const encryptedPath = await mergeEncryptedFile(
      packageDirs,
      manifest,
      artifactName,
      restoredOutputDir
    );

    if (SKIP_DECRYPT) {
      console.log(`已输出加密文件: ${encryptedPath}`);
    } else {
      const restoredPath = await decryptFile(
        encryptedPath,
        manifest,
        restoredOutputDir
      );
      console.log(`已恢复原文件: ${restoredPath}`);

      if (!KEEP_ENCRYPTED) {
        await fsp.rm(encryptedPath, { force: true });
      }
    }

    if (!SOURCE_DIR) {
      await cleanupTemp(tempRoot);
    }
  } finally {
    await cleanupTemp(tempBase);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
