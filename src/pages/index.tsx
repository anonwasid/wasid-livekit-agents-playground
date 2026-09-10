import { AnimatePresence, motion } from "framer-motion";
import { Inter } from "next/font/google";
import Head from "next/head";
import React, { useState, useEffect } from "react";

import { PlaygroundConnect } from "@/components/PlaygroundConnect";
import Playground from "@/components/playground/Playground";
import { PlaygroundToast } from "@/components/toast/PlaygroundToast";
import { ConfigProvider, useConfig } from "@/hooks/useConfig";
import { ToastProvider, useToast } from "@/components/toast/ToasterProvider";
import { TokenSourceConfigurable, TokenSource } from "livekit-client";

const themeColors = [
  "cyan",
  "green",
  "amber",
  "blue",
  "violet",
  "rose",
  "pink",
  "teal",
];

const inter = Inter({ subsets: ["latin"] });

export default function Home() {
  return (
    <ToastProvider>
      <ConfigProvider>
        <HomeInner />
      </ConfigProvider>
    </ToastProvider>
  );
}

export function HomeInner() {
  const { config } = useConfig();
  const { toastMessage } = useToast();
  const [autoConnect, setAutoConnect] = useState(false);
  
  // Authentication states
  const [authChecked, setAuthChecked] = useState(false);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [loginError, setLoginError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const [tokenSource, setTokenSource] = useState<
    TokenSourceConfigurable | undefined
  >(() => {
    if (process.env.NEXT_PUBLIC_LIVEKIT_URL) {
      return TokenSource.endpoint("/api/token");
    }
    return undefined;
  });

  useEffect(() => {
    fetch("/api/auth/check")
      .then((res) => res.json())
      .then((data) => {
        setIsAuthenticated(Boolean(data.authenticated));
        setAuthChecked(true);
      })
      .catch(() => {
        setIsAuthenticated(false);
        setAuthChecked(true);
      });
  }, []);

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsSubmitting(true);
    setLoginError("");
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      });
      const data = await res.json();
      if (res.ok) {
        setIsAuthenticated(true);
        setUsername("");
        setPassword("");
      } else {
        setLoginError(data.error || "Invalid username or password");
      }
    } catch {
      setLoginError("Network connection error");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleLogout = async () => {
    await fetch("/api/auth/logout", { method: "POST" });
    setIsAuthenticated(false);
    setUsername("");
    setPassword("");
    setLoginError("");
  };

  return (
    <>
      <Head>
        <title>{config.title} | WASID Voice</title>
        <meta name="description" content={config.description} />
        <meta
          name="viewport"
          content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no"
        />
        <meta name="apple-mobile-web-app-capable" content="yes" />
        <meta name="apple-mobile-web-app-status-bar-style" content="black" />
        <link rel="icon" href="/favicon.ico" />
      </Head>

      <main className="relative flex flex-col justify-center px-4 items-center h-full w-full bg-black repeating-square-background font-sans">
        <AnimatePresence>
          {toastMessage && (
            <motion.div
              className="left-0 right-0 top-0 absolute z-10"
              initial={{ opacity: 0, translateY: -50 }}
              animate={{ opacity: 1, translateY: 0 }}
              exit={{ opacity: 0, translateY: -50 }}
            >
              <PlaygroundToast />
            </motion.div>
          )}
        </AnimatePresence>

        {!authChecked ? (
          <div className="flex items-center space-x-2 text-neutral-400 text-sm">
            <div className="w-4 h-4 rounded-full border-2 border-cyan-500 border-t-transparent animate-spin"></div>
            <span>Verifying session...</span>
          </div>
        ) : !isAuthenticated ? (
          <div className="w-full max-w-md p-8 rounded-2xl bg-[#0f1117] border border-neutral-800 shadow-2xl space-y-6 z-20">
            <div className="text-center space-y-2">
              <div className="w-12 h-12 rounded-xl bg-gradient-to-tr from-cyan-600 to-blue-500 mx-auto flex items-center justify-center font-bold text-xl text-white shadow-lg">
                LK
              </div>
              <h2 className="text-lg font-bold text-white tracking-wide">
                LiveKit Voice Playground
              </h2>
              <p className="text-xs text-neutral-400">
                WASID Phase 7 Voice Platform • Authenticated Edge Console
              </p>
            </div>

            {loginError && (
              <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-xs text-center font-medium">
                {loginError}
              </div>
            )}

            <form onSubmit={handleLogin} className="space-y-4 text-xs">
              <div>
                <label className="block font-semibold text-neutral-300 mb-1">
                  Operator Username
                </label>
                <input
                  type="text"
                  required
                  autoComplete="username"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  placeholder="name@domain.com"
                  className="w-full p-2.5 rounded-lg bg-neutral-900 border border-neutral-700 text-white placeholder-neutral-500 focus:outline-none focus:border-cyan-500 text-sm"
                />
              </div>

              <div>
                <label className="block font-semibold text-neutral-300 mb-1">
                  Password
                </label>
                <input
                  type="password"
                  required
                  autoComplete="current-password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••••••"
                  className="w-full p-2.5 rounded-lg bg-neutral-900 border border-neutral-700 text-white placeholder-neutral-500 focus:outline-none focus:border-cyan-500 text-sm"
                />
              </div>

              <button
                type="submit"
                disabled={isSubmitting}
                className="w-full py-2.5 rounded-lg bg-cyan-600 hover:bg-cyan-500 font-bold text-white text-xs transition shadow-lg disabled:opacity-50"
              >
                {isSubmitting ? "Authenticating..." : "Sign In to LiveKit Voice"}
              </button>
            </form>

            <p className="text-[11px] text-center text-neutral-500">
              Restricted access. Credentials configured in Coolify material.
            </p>
          </div>
        ) : (
          <>
            <button
              onClick={handleLogout}
              className="absolute top-4 right-4 z-50 text-xs px-3 py-1.5 rounded-lg bg-neutral-900/80 hover:bg-neutral-800 border border-neutral-700 text-neutral-300 hover:text-red-400 transition backdrop-blur-sm"
            >
              Sign Out
            </button>

            {tokenSource ? (
              <Playground
                themeColors={themeColors}
                tokenSource={tokenSource}
                autoConnect={autoConnect}
                agentOptions={
                  config.settings.agent
                    ? { agentName: config.settings.agent }
                    : config.agent_dispatch
                }
              />
            ) : (
              <PlaygroundConnect
                accentColor={themeColors[0]}
                onConnectClicked={(tokenSource, shouldAutoConnect) => {
                  setTokenSource(tokenSource);
                  if (shouldAutoConnect) {
                    setAutoConnect(true);
                  }
                }}
              />
            )}
          </>
        )}
      </main>
    </>
  );
}
