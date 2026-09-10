import crypto from "crypto";

const SECRET =
  process.env.PLAYGROUND_SESSION_SECRET ||
  "wasid_livekit_voice_session_secret_2026_secure";

export function signSession(user: string): string {
  const expires = Date.now() + 24 * 60 * 60 * 1000;
  const payload = `${user}:${expires}`;
  const sig = crypto.createHmac("sha256", SECRET).update(payload).digest("hex");
  return Buffer.from(`${payload}:${sig}`).toString("base64");
}

export function verifySession(token: string | undefined): {
  valid: boolean;
  user?: string;
} {
  if (!token) return { valid: false };
  try {
    const raw = Buffer.from(token, "base64").toString("utf-8");
    const parts = raw.split(":");
    if (parts.length !== 3) return { valid: false };
    const [user, expiresStr, sig] = parts;
    const expires = parseInt(expiresStr, 10);
    if (isNaN(expires) || Date.now() > expires) return { valid: false };
    const expectedSig = crypto
      .createHmac("sha256", SECRET)
      .update(`${user}:${expiresStr}`)
      .digest("hex");
    if (
      sig.length === expectedSig.length &&
      crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expectedSig))
    ) {
      return { valid: true, user };
    }
    return { valid: false };
  } catch {
    return { valid: false };
  }
}

export function parseCookies(
  cookieHeader: string | undefined
): Record<string, string> {
  const list: Record<string, string> = {};
  if (!cookieHeader) return list;
  cookieHeader.split(";").forEach((cookie) => {
    const parts = cookie.split("=");
    if (parts.length >= 2) {
      list[parts[0].trim()] = decodeURIComponent(
        parts.slice(1).join("=").trim()
      );
    }
  });
  return list;
}
