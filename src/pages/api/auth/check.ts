import type { NextApiRequest, NextApiResponse } from "next";
import { verifySession, parseCookies } from "@/lib/auth";

export default async function handleCheck(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const cookies = parseCookies(req.headers.cookie);
  const sessionToken = cookies["lkvoice_session"];
  const result = verifySession(sessionToken);

  if (result.valid) {
    return res.status(200).json({ authenticated: true, user: result.user });
  }
  return res.status(200).json({ authenticated: false });
}
