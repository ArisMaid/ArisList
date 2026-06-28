import { type ReactNode } from "react";
import { GLASS_FILTER_ID } from "./GlassSurface";

export function GlassFilterProvider({ children }: { children: ReactNode }) {
  return (
    <>
      <svg className="glass-filter-root" aria-hidden="true" focusable="false">
        <defs>
          <filter id={GLASS_FILTER_ID} x="-18%" y="-18%" width="136%" height="136%" colorInterpolationFilters="sRGB">
            <feTurbulence type="fractalNoise" baseFrequency="0.018 0.026" numOctaves="2" seed="11" result="noise" />
            <feDisplacementMap in="SourceGraphic" in2="noise" scale="18" xChannelSelector="R" yChannelSelector="G" result="displaced" />
            <feColorMatrix
              in="displaced"
              type="matrix"
              values="1 0 0 0 0  0 0.98 0 0 0  0 0 1.04 0 0  0 0 0 1 0"
              result="aberrated"
            />
            <feGaussianBlur in="aberrated" stdDeviation="0.18" />
          </filter>
        </defs>
      </svg>
      {children}
    </>
  );
}

