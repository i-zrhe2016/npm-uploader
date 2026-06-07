# npm-uploader

把大文件先加密，再切成多个分片，并将每个分片封装成独立的 npm 包发布；之后可以从本地分片目录或 npm registry 拉回这些包，重新合并并解密出原文件。

这个仓库当前只有两个核心脚本：

- `shard-upload.js`：扫描 `files/` 目录中的文件，执行加密、切片、生成 npm 包、发布、写入记录。
- `shard-restore.js`：根据本地分片目录、`manifest.json` 或 `upload-records.json`，把分片重新下载/合并/解密。

## 工作流程

1. 原文件放进 `files/`
2. `shard-upload.js` 使用 `AES-256-GCM` 加密文件
3. 按 `SHARD_SIZE_BYTES` 切成多个 `.part` 文件
4. 为每个分片生成一个 npm 包：`@scope/<artifact>-shard-<index>`
5. 发布成功后，把分片元数据写入：
   - `shards/<artifact>/manifest.json`
   - `upload-records.json`
6. `shard-restore.js` 可基于这些元数据从本地或 npm 恢复原文件

## 运行环境

- Node.js：当前仓库在本地用 `v24.15.0` 验证过
- npm：当前仓库在本地用 `11.12.1` 验证过
- 需要系统可用 `tar`
- 如果要发布到 npm，需要先完成 `npm login`

## 快速开始

### 1. 准备待上传文件

把文件放到 `files/` 目录，例如：

```bash
mkdir -p files
cp /path/to/your-file.bin files/
```

### 2. 配置环境变量

至少建议设置这几个：

```bash
export SHARD_PASSWORD='replace-with-a-real-password'
export NPM_SCOPE='@your-npm-scope'
export NPM_ACCESS='public'
export SHARD_SIZE_BYTES=$((5 * 1024 * 1024))
```

如果你不想真的发布到 npm，可以先 dry-run：

```bash
export DRY_RUN=1
```

### 3. 上传

```bash
node shard-upload.js
```

脚本会扫描 `files/` 下的所有普通文件，逐个处理。

### 4. 从本地分片恢复

```bash
export SHARD_PASSWORD='replace-with-the-same-password'
node shard-restore.js \
  --artifact your-file-name \
  --source-dir shards/your-file-name \
  --output-dir restored
```

恢复结果默认输出到：

```text
restored/<artifact>/
```

### 5. 从 npm 恢复

如果本地没有分片目录，可以依赖 `upload-records.json` 或手动指定 `manifest.json`：

```bash
export SHARD_PASSWORD='replace-with-the-same-password'
export NPM_SCOPE='@your-npm-scope'
node shard-restore.js --artifact your-file-name
```

或：

```bash
node shard-restore.js --manifest /path/to/manifest.json
```

## 目录结构

上传后通常会生成如下结构：

```text
files/
  your-file.bin

shards/
  your-file/
    manifest.json
    your-file.part0
    your-file.part1
    ...
    shard-0/
      package.json
      index.js
      manifest.json
      your-file.part0
    shard-1/
      ...

upload-records.json
```

其中：

- `manifest.json` 保存单次上传的完整元数据，包括加密参数、分片信息、发布结果
- `upload-records.json` 保存每个 artifact 的 `latest` 和有限历史记录

## 上传脚本配置

`shard-upload.js` 不读取命令行参数，全部通过环境变量控制。

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `FILES_DIR` | `files` | 待上传文件目录 |
| `SHARDS_DIR` | `shards` | 分片与生成包目录 |
| `SHARD_PASSWORD` | 内置占位密码 | 加密密码，必须自行替换 |
| `SHARD_SIZE_BYTES` | `5242880` | 单分片大小，默认 5MB |
| `UPLOAD_RECORD_PATH` / `UPLOAD_RECORD_FILE` | `upload-records.json` | 记录文件路径 |
| `UPLOAD_RECORD_HISTORY_LIMIT` | `20` | 每个 artifact 保留的历史条数 |
| `NPM_SCOPE` | `@yourusername` | npm scope |
| `NPM_ACCESS` | `public` | `npm publish --access` 参数 |
| `NPM_PACKAGE_VERSION` | `1.0.0` | 分片包版本号 |
| `NPM_REGISTRY` | `https://registry.npmjs.org` | npm registry |
| `NPM_WEB_BASE` | `https://www.npmjs.com/package` | 生成包详情页链接用 |
| `NPM_TAG` | `latest` | npm dist-tag |
| `NPM_PUBLISH_OTP` | 空 | npm 2FA OTP |
| `PUBLISH_CONCURRENCY` | `2` | 并发发布数 |
| `NPM_PUBLISH_TIMEOUT_MS` | `600000` | 单次发布超时 |
| `DRY_RUN` | 未开启 | `1` 时跳过真实发布 |

## 恢复脚本参数

`shard-restore.js` 支持命令行参数，也支持对应环境变量。

| 参数 | 说明 |
| --- | --- |
| `--artifact` | artifact 名称，通常是原文件名去掉扩展名后的安全化结果 |
| `--manifest` | 指定单个 `manifest.json` |
| `--record` | 指定记录文件，默认 `upload-records.json` |
| `--scope` | npm scope |
| `--version` | 默认分片版本，未从 manifest 中解析到时使用 |
| `--source-dir` | 直接从本地分片目录恢复，不走 npm 下载 |
| `--output-dir` | 输出目录，默认 `restored` |
| `--password` | 覆盖 `SHARD_PASSWORD` |
| `--download-concurrency` | 并发下载数，默认 `4` |
| `--registry` | 自定义 registry |
| `--timeout` | npm/tar 命令超时 |
| `--keep-temp` | 保留下载临时目录 |
| `--keep-encrypted` | 解密完成后保留中间 `.enc` 文件 |
| `--skip-decrypt` | 只合并分片，不执行解密 |

## 恢复策略说明

恢复过程分成两部分：

### 1. 元数据来源

- 传了 `--manifest`：直接读取该 manifest
- 否则如果记录文件存在：读取 `upload-records.json` 或 `--record` 指定文件
- 否则：不预先读取元数据，后续从第一个分片包中取 `manifest.json`

如果记录文件里包含多个 artifact，需要显式传 `--artifact`。

### 2. 分片来源

- 传了 `--source-dir`：直接从本地 `shard-*` 目录读取分片
- 没传 `--source-dir`：根据 scope、artifact、version 从 npm 下载分片包

## 产物命名规则

- artifact 名称来自原文件名，转成小写，并把非 `a-z0-9._-` 字符替换成 `-`
- 分片文件名格式：`<artifact>.part<index>`
- npm 包名格式：`@scope/<artifact>-shard-<index>`

## 安全与使用注意

- 默认密码 `your-strong-password-here-123456` 只是占位值，不应在真实环境使用
- 上传和恢复必须使用同一份 `SHARD_PASSWORD`，否则会在解密阶段报认证失败
- `manifest.json` 中包含 `salt`、`iv`、`authTag` 等解密必需信息，应和密码一起妥善管理
- npm 上同一个包版本不能重复发布；重复上传时需要调整 `NPM_PACKAGE_VERSION`
- 如果启用了 npm 2FA，发布时需要提供 `NPM_PUBLISH_OTP`
- 当前上传脚本会清空目标 `shards/<artifact>/` 目录后重新生成

## 示例数据

仓库里带了一个示例目录：

```text
shards/test-upload-5/
```

它可以用于查看分片结构和记录格式；但如果没有对应的真实密码，不能成功解密出原文件。

## 常用命令

```bash
# 本地试跑，不真正 publish
DRY_RUN=1 SHARD_PASSWORD='test-password' NPM_SCOPE='@example' node shard-upload.js

# 基于本地分片目录恢复
SHARD_PASSWORD='test-password' node shard-restore.js \
  --artifact your-file \
  --source-dir shards/your-file

# 只合并加密文件，不解密
SHARD_PASSWORD='test-password' node shard-restore.js \
  --artifact your-file \
  --source-dir shards/your-file \
  --skip-decrypt
```
