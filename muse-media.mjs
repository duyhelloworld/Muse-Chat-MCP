/**
 * muse-media.mjs
 * ------------------------------------------------------------------
 * Pure image helpers for muse_media (no browser, no Muse network):
 *   toPng    - decode webp/jpeg/png and re-encode PNG, longest side <= maxSize,
 *              so MCP clients get an image block they can read directly
 *              (no more `sips -s format png` on the caller's side).
 *   compare  - how much two images differ. Muse often says "done, I edited it"
 *              while the image is unchanged; changedPct ~ 0 exposes that.
 *   hashHex  - 64-bit dHash as a 16-hex token, to compare across calls/turns.
 * ------------------------------------------------------------------
 */
import sharp from 'sharp'

export const DEFAULT_MAX_SIZE = 1024
// Mean per-pixel difference (0..100) at or above which an image counts as changed.
// Re-encoding the same picture lands well under 1; a real edit lands well above.
export const CHANGED_THRESHOLD_PCT = 1.0

/** Re-encode as PNG, downscaled (never upscaled) so max(width, height) <= maxSize. */
export async function toPng(input, maxSize = DEFAULT_MAX_SIZE) {
  if (!Buffer.isBuffer(input) || !input.length) throw new Error('toPng: empty image buffer')
  const limit = Number.isFinite(maxSize) && maxSize > 0 ? Math.floor(maxSize) : DEFAULT_MAX_SIZE
  const png = await sharp(input, { failOn: 'none' })
    .resize({ width: limit, height: limit, fit: 'inside', withoutEnlargement: true })
    .png()
    .toBuffer({ resolveWithObject: true })
  return { buffer: png.data, mimeType: 'image/png', width: png.info.width, height: png.info.height, bytes: png.data.length }
}

/** Source dimensions without decoding the whole image. */
export async function dimensions(input) {
  const m = await sharp(input, { failOn: 'none' }).metadata()
  return { width: m.width || 0, height: m.height || 0 }
}

const grayRaw = async (input, w, h) =>
  (await sharp(input, { failOn: 'none' }).resize(w, h, { fit: 'fill' }).grayscale().raw().toBuffer({ resolveWithObject: true })).data

/** dHash: each pixel vs its right neighbour on a 9x8 grayscale grid = 64 bits. */
async function dHash(input) {
  const data = await grayRaw(input, 9, 8)
  let hash = 0n
  let bit = 0n
  for (let row = 0; row < 8; row++) {
    for (let col = 0; col < 8; col++) {
      if (data[row * 9 + col] > data[row * 9 + col + 1]) hash |= 1n << bit
      bit++
    }
  }
  return hash
}

const popcount = (x) => { let n = 0; while (x > 0n) { n += Number(x & 1n); x >>= 1n } return n }

/**
 * changedPct: mean absolute difference on a 32x32 grayscale downscale, 0..100 (headline metric).
 * dhashHamming: dHash distance 0..64, a cross-check that ignores re-encode noise.
 */
export async function compare(prev, next) {
  const [a, b] = await Promise.all([grayRaw(prev, 32, 32), grayRaw(next, 32, 32)])
  let sum = 0
  for (let i = 0; i < a.length; i++) sum += Math.abs(a[i] - b[i])
  const changedPct = Math.round((sum / a.length / 255) * 100 * 100) / 100
  const [ha, hb] = await Promise.all([dHash(prev), dHash(next)])
  return { changedPct, dhashHamming: popcount(ha ^ hb) }
}

export const hashHex = async (input) => (await dHash(input)).toString(16).padStart(16, '0')

export const hammingHex = (a, b) => popcount(BigInt('0x' + a) ^ BigInt('0x' + b))
