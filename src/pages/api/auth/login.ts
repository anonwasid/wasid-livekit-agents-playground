import type { NextApiRequest, NextApiResponse } from "next";
import { signSession } from "@/lib/auth";

export default async function handleLogin(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const { username, password } = req.body || {};

  const expectedUser = process.env.PLAYGROUND_ADMIN_USERNAME || "admin@wasidai.com";
  const expectedPass = process.env.PLAYGROUND_ADMIN_PASSWORD || "WasidLiveKit2026!";

  if (
    !username ||
    !password ||
    username.trim() !== expectedUser.trim() ||
    password !== expectedPass
  ) {
    return res.status(401).json({ error: "Invalid username or password" });
  }

  const token = signSession(username);

  res.setHeader(
    "Set-Cookie",
    `lkvoice_session=${token}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=86400`
  );

  return res.status(200).json({ ok: true, user: username });
}
