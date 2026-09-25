'use strict';

const fs = require('node:fs');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { PNG } = require('pngjs');
const run = promisify(execFile);

function mediaPaths() {
  return {
    ffmpeg: process.env.FFMPEG_PATH || require('ffmpeg-static'),
    ffprobe: process.env.FFPROBE_PATH || require('ffprobe-static').path
  };
}

function validatePngContent(file, expected = {}) {
  try {
    // Full decompression and CRC checking; a header alone is not an image.
    const png = PNG.sync.read(fs.readFileSync(file), { checkCRC: true });
    return png.width > 0 && png.height > 0 &&
      (!expected.width || png.width === expected.width) &&
      (!expected.height || png.height === expected.height) &&
      png.data.length === png.width * png.height * 4;
  } catch { return false; }
}

async function inspectMp4(file, expected = {}) {
  const { ffprobe, ffmpeg } = mediaPaths();
  const options = { windowsHide: true, timeout: 120000, maxBuffer: 1024 * 1024 };
  const { stdout } = await run(ffprobe, [
    '-v', 'error', '-count_frames', '-select_streams', 'v:0',
    '-show_entries', 'stream=codec_name,width,height,nb_read_frames,duration:format=duration',
    '-of', 'json', file
  ], options);
  const info = JSON.parse(stdout);
  const video = info.streams?.[0];
  const duration = Number(video?.duration || info.format?.duration);
  const frames = Number(video?.nb_read_frames);
  if (!video || !(video.width > 0 && video.height > 0 && frames > 0 && duration > 0) ||
      (expected.width && video.width !== expected.width) ||
      (expected.height && video.height !== expected.height) ||
      (expected.frames && frames !== expected.frames) ||
      (expected.seconds && Math.abs(duration - expected.seconds) > 0.1)) {
    throw new Error('MP4 sin vídeo completo o con dimensiones/duración incorrectas');
  }
  const decoded = await run(ffmpeg, [
    '-hide_banner', '-v', 'error', '-xerror', '-err_detect', 'explode',
    '-i', file, '-map', '0:v:0', '-f', 'null', '-'
  ], options);
  if (decoded.stderr.trim()) throw new Error(`MP4 no decodificable: ${decoded.stderr}`);
  return { width: video.width, height: video.height, frames, duration };
}

async function validateMp4Content(file, expected = {}) {
  try { await inspectMp4(file, expected); return true; }
  catch { return false; } // Missing tools also fail closed; never accept by extension/header.
}

module.exports = { mediaPaths, validatePngContent, validateMp4Content, inspectMp4 };
