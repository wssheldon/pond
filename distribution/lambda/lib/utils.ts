import * as path from "path";
import { execSync } from "child_process";

export function getGitRootPath(): string {
  try {
    const gitRoot = execSync("git rev-parse --show-toplevel", {
      encoding: "utf-8",
    }).trim();
    return path.resolve(gitRoot);
  } catch (error) {
    console.error("Failed to get git root:", error);
    throw error;
  }
}

export function getAssetPath(...paths: string[]): string {
  return path.join(getGitRootPath(), ...paths);
}
