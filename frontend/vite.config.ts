import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.indexOf("node_modules/motion") >= 0 || id.indexOf("node_modules/framer-motion") >= 0) return "motion";
          if (id.indexOf("node_modules/react") >= 0 || id.indexOf("node_modules/react-dom") >= 0) return "react";
          return undefined;
        }
      }
    }
  },
  resolve: {
    alias: {
      "@pdfjs/pdf.min.mjs": new URL("./src/vendor/pdfjs-stub.ts", import.meta.url).pathname
    }
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8787"
    }
  }
});
