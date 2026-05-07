#!/usr/bin/env node
/**
 * Post-install script: downloads the correct pre-built binary for the current platform.
 */
const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { execSync } = require('child_process');

const REPO = 'The-AgenticFlow/AgentFlow';
const BIN_DIR = path.join(__dirname, '..', 'bin');

function detectPlatform() {
    const platform = os.platform();
    const arch = os.arch();

    let osPart, archPart;

    switch (platform) {
        case 'darwin':
            osPart = 'apple-darwin';
            break;
        case 'linux':
            // Check for musl
            try {
                const ldd = execSync('ldd --version 2>&1', { encoding: 'utf8' });
                if (ldd.includes('musl')) {
                    osPart = 'unknown-linux-musl';
                } else {
                    osPart = 'unknown-linux-gnu';
                }
            } catch {
                osPart = 'unknown-linux-gnu';
            }
            break;
        default:
            console.error(`Unsupported platform: ${platform}`);
            process.exit(1);
    }

    switch (arch) {
        case 'x64':
            archPart = 'x86_64';
            break;
        case 'arm64':
            archPart = 'aarch64';
            break;
        default:
            console.error(`Unsupported architecture: ${arch}`);
            process.exit(1);
    }

    return `${archPart}-${osPart}`;
}

function download(url, dest) {
    return new Promise((resolve, reject) => {
        const parsed = new URL(url);
        const client = parsed.protocol === 'https:' ? https : http;
        const file = fs.createWriteStream(dest);

        client.get(url, (response) => {
            if (response.statusCode === 302 || response.statusCode === 301) {
                download(response.headers.location, dest).then(resolve).catch(reject);
                return;
            }
            if (response.statusCode !== 200) {
                reject(new Error(`HTTP ${response.statusCode}: ${response.statusMessage}`));
                return;
            }
            response.pipe(file);
            file.on('finish', () => {
                file.close();
                resolve();
            });
        }).on('error', (err) => {
            fs.unlink(dest, () => {});
            reject(err);
        });
    });
}

function extractTarGz(tarPath, destDir) {
    return new Promise((resolve, reject) => {
        const tar = require('child_process').spawn('tar', ['-xzf', tarPath, '-C', destDir, '--strip-components=1']);
        tar.on('close', (code) => {
            if (code === 0) resolve();
            else reject(new Error(`tar exited with code ${code}`));
        });
    });
}

async function main() {
    const platform = detectPlatform();
    console.log(`[@the-agenticflow/openflows] Downloading binary for ${platform}...`);

    // Ensure bin directory exists
    if (!fs.existsSync(BIN_DIR)) {
        fs.mkdirSync(BIN_DIR, { recursive: true });
    }

    // Get latest release tag with better error handling
    let tag;
    try {
        tag = await new Promise((resolve, reject) => {
            const req = https.get(`https://api.github.com/repos/${REPO}/releases/latest`, {
                headers: { 
                    'User-Agent': 'openflows-npm-installer',
                    'Accept': 'application/vnd.github.v3+json'
                }
            }, (res) => {
                if (res.statusCode !== 200) {
                    reject(new Error(`GitHub API returned ${res.statusCode}`));
                    return;
                }
                let data = '';
                res.on('data', (chunk) => data += chunk);
                res.on('end', () => {
                    try {
                        const json = JSON.parse(data);
                        if (!json.tag_name) {
                            reject(new Error('No tag_name in release response'));
                        } else {
                            resolve(json.tag_name);
                        }
                    } catch (parseErr) {
                        reject(new Error(`Failed to parse release info: ${parseErr.message}`));
                    }
                });
            });
            req.on('error', reject);
            req.setTimeout(30000, () => {
                req.destroy();
                reject(new Error('GitHub API request timeout'));
            });
        });
    } catch (apiErr) {
        console.error(`[@the-agenticflow/openflows] GitHub API error: ${apiErr.message}`);
        console.error('[@the-agenticflow/openflows] Falling back to latest known version: v0.1.3');
        tag = 'v0.1.3';
    }

    const archiveName = `openflows-${tag}-${platform}.tar.gz`;
    const downloadUrl = `https://github.com/${REPO}/releases/download/${tag}/${archiveName}`;
    // Use package's temp directory instead of system /tmp to avoid permission issues
    const tmpDir = path.join(__dirname, '..', '.tmp');
    if (!fs.existsSync(tmpDir)) {
        fs.mkdirSync(tmpDir, { recursive: true });
    }
    const tmpFile = path.join(tmpDir, archiveName);

    try {
        await download(downloadUrl, tmpFile);
        await extractTarGz(tmpFile, BIN_DIR);
    } catch (err) {
        // For x86_64 Linux, try musl fallback
        if (platform === 'x86_64-unknown-linux-gnu') {
            const muslArchiveName = `openflows-${tag}-x86_64-unknown-linux-musl.tar.gz`;
            const muslDownloadUrl = `https://github.com/${REPO}/releases/download/${tag}/${muslArchiveName}`;
            const muslTmpFile = path.join(tmpDir, muslArchiveName);
            console.log(`[@the-agenticflow/openflows] Trying musl fallback...`);
            await download(muslDownloadUrl, muslTmpFile);
            await extractTarGz(muslTmpFile, BIN_DIR);
            fs.unlinkSync(muslTmpFile);
        } else {
            throw err;
        }
    }

    // Rename binaries to match expected names
    const binaries = ['agentflow', 'agentflow-setup', 'agentflow-dashboard', 'agentflow-doctor'];
    for (const bin of binaries) {
        const src = path.join(BIN_DIR, bin);
        const dst = path.join(BIN_DIR, `${bin}-bin`);
        if (fs.existsSync(src)) {
            fs.renameSync(src, dst);
            fs.chmodSync(dst, 0o755);
        }
    }

    if (fs.existsSync(tmpFile)) {
        fs.unlinkSync(tmpFile);
    }
    // Clean up temp directory if empty
    try {
        if (fs.existsSync(tmpDir) && fs.readdirSync(tmpDir).length === 0) {
            fs.rmdirSync(tmpDir);
        }
    } catch (cleanupErr) {
        // Ignore cleanup errors
    }
    console.log(`[openflows] Installation complete!`);
}

main();
