import { createElement, forwardRef, type HTMLAttributes, type ReactNode } from "react";

export type GlassSurfaceVariant = "panel" | "floating" | "control" | "dock";

export type GlassSurfaceProps = HTMLAttributes<HTMLDivElement> & {
  as?: "article" | "aside" | "div" | "header";
  variant?: GlassSurfaceVariant;
  selected?: boolean;
  children: ReactNode;
};

export const GLASS_FILTER_ID = "aris-liquid-glass-filter";

export const GlassSurface = forwardRef<HTMLDivElement, GlassSurfaceProps>(function GlassSurface(
  { as = "div", variant = "panel", selected = false, className, children, ...rest },
  ref
) {
  const classes = [
    "glass-surface",
    `glass-surface-${variant}`,
    selected ? "glass-surface-selected" : "",
    className ?? ""
  ]
    .filter(Boolean)
    .join(" ");

  return createElement(
    as,
    { ref, className: classes, ...rest },
    <>
      <div className="glass-surface-backdrop" aria-hidden="true" />
      {(variant === "floating" || variant === "dock") && (
        <div className="glass-surface-refraction" style={{ filter: `url(#${GLASS_FILTER_ID})` }} aria-hidden="true" />
      )}
      <div className="glass-surface-edge" aria-hidden="true" />
      {selected && <div className="glass-surface-ring" aria-hidden="true" />}
      <div className="glass-surface-content">{children}</div>
    </>
  );
});
