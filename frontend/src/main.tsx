import React from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { MotionProvider } from "./ui/MotionProvider";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <MotionProvider>
      <App />
    </MotionProvider>
  </React.StrictMode>
);
