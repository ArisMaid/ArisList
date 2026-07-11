import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
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
