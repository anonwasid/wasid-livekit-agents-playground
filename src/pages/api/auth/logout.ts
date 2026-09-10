import type { NextApiRequest, NextApiResponse } from "next";

export default async function handleLogout(
  req: NextApiRequest,
  res: NextApiResponse
) {
  res.setHeader(
    "Set-Cookie",
    `lkvoice_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0`
  );
  return res.status(200).json({ ok: true });
}
