import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./App";
import { PlayerUiSandbox } from "./components/playback/PlayerUiSandbox";
import "./styles.css";

const searchParams = new URLSearchParams(window.location.search);
const rootComponent = searchParams.get("playerSandbox") === "1" ? <PlayerUiSandbox /> : <App />;

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    {rootComponent}
  </React.StrictMode>,
);
