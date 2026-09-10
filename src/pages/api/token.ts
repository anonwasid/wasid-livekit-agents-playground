import { NextApiRequest, NextApiResponse } from "next";

import { AccessToken } from "livekit-server-sdk";
import { RoomConfiguration } from "@livekit/protocol";
import { verifySession, parseCookies } from "@/lib/auth";

const apiKey = process.env.LIVEKIT_API_KEY;
const apiSecret = process.env.LIVEKIT_API_SECRET;

type TokenRequest = {
  room_name: string;
  participant_identity: string;
  participant_name?: string;
  participant_metadata?: string;
  participant_attributes?: Record<string, string>;
  room_config?: ReturnType<RoomConfiguration["toJson"]>;
};

async function createToken(request: TokenRequest) {
  const at = new AccessToken(
    process.env.LIVEKIT_API_KEY,
    process.env.LIVEKIT_API_SECRET,
    {
      identity: request.participant_identity,
      ttl: "10m",
    }
  );

  at.addGrant({
    roomJoin: true,
    room: request.room_name,
    canUpdateOwnMetadata: true,
  });

  if (request.participant_name) {
    at.name = request.participant_name;
  }
  if (request.participant_identity) {
    at.identity = request.participant_identity;
  }
  if (request.participant_metadata) {
    at.metadata = request.participant_metadata;
  }
  if (request.participant_attributes) {
    at.attributes = request.participant_attributes;
  }
  if (request.room_config) {
    at.roomConfig = RoomConfiguration.fromJson(request.room_config);
  }

  return at.toJwt();
}

export default async function handleToken(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    res.status(405).end("Method Not Allowed");
    return;
  }

  // Enforce session authentication before token issuance
  const cookies = parseCookies(req.headers.cookie);
  const sessionToken = cookies["lkvoice_session"];
  const auth = verifySession(sessionToken);
  if (!auth.valid) {
    return res
      .status(401)
      .json({ message: "Authentication required to generate WebRTC tokens." });
  }

  if (!apiKey || !apiSecret) {
    res.statusMessage = "Environment variables aren't set up correctly";
    res.status(500).end();
    return;
  }

  const options = req.body ?? {};
  const suffix = crypto.randomUUID().substring(0, 8);
  options.room_name = options.room_name ?? options.roomName ?? `room-${suffix}`;
  options.participant_identity =
    options.participant_identity ?? options.participantName ?? `user-${suffix}`;

  try {
    res.status(200).json({
      server_url: process.env.NEXT_PUBLIC_LIVEKIT_URL,
      participant_token: await createToken(options),
    });
  } catch (err) {
    console.error("Error generating token:", err);
    res.status(500).send({ message: "Generating token failed" });
  }
}
