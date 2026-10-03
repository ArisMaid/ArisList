export const uiEaseOut = [0.23, 1, 0.32, 1] as const;
export const uiEaseStandard = [0.2, 0.65, 0.3, 1] as const;

export const uiDuration = {
  press: 0.14,
  fade: 0.16,
  popover: 0.18,
  modal: 0.22,
  drawer: 0.26
} as const;

export const uiSpring = {
  type: "spring" as const,
  stiffness: 430,
  damping: 34,
  mass: 0.8
};
