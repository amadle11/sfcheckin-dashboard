import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  serverExternalPackages: ["@google-cloud/bigquery", "@google-cloud/firestore"],
};

export default nextConfig;
