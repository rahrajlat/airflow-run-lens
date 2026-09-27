import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "path";

export default defineConfig({
  plugins: [react()],
  build: {
    emptyOutDir: false,
    lib: {
      entry: resolve(__dirname, "src/main.tsx"),
      name: "RunLensBundle",
      formats: ["iife"],
      fileName: () => "runlens.js"
    },
    outDir: "../../plugins/runlens",
    rollupOptions: {
      external: ["react"],
      output: {
        assetFileNames: "assets/[name][extname]",
        globals: {
          react: "React"
        }
      }
    }
  },
  define: {
    "process.env.NODE_ENV": JSON.stringify("production")
  }
});
