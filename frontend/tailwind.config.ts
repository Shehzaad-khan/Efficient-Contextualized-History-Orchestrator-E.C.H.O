import type { Config } from 'tailwindcss'

/**
 * ECHO design tokens — "The Midnight Study".
 * Warm ink-black surfaces, cream typographic ink, lamplight-amber accent,
 * per-source hues. Everything the UI uses is declared here; default Tailwind
 * grays/blues are intentionally not part of the visual language.
 */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        // Surfaces — warm near-blacks (espresso, not blue-gray)
        night: {
          950: '#0C0A07', // page base
          900: '#12100C', // raised surface
          850: '#181510', // card
          800: '#1F1B15', // hover / inset
          700: '#2B2620', // strong border / divider
        },
        // Ink — warm creams
        ink: {
          DEFAULT: '#EDE6D8',
          soft: '#C9C0AF',
          mute: '#8D8577',
          faint: '#5C564B',
        },
        // Lamplight — the single accent
        ember: {
          300: '#FFD9A0',
          400: '#FFC46E',
          500: '#F5A947',
          600: '#D98A2B',
        },
        // Source identities (desaturated to sit in the same room)
        gmail: '#E88A70',
        chrome: '#8FB8E8',
        youtube: '#E87085',
        // Semantic
        moss: '#9DBF8E', // positive / saved
      },
      fontFamily: {
        display: ['"Fraunces Variable"', 'Georgia', 'serif'],
        body: ['"Instrument Sans Variable"', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono Variable"', 'ui-monospace', 'monospace'],
      },
      fontSize: {
        // Fluid display scale
        'title-xl': ['clamp(2.5rem, 5vw, 4.25rem)', { lineHeight: '1.02', letterSpacing: '-0.02em' }],
        'title-lg': ['clamp(1.8rem, 3.2vw, 2.6rem)', { lineHeight: '1.08', letterSpacing: '-0.015em' }],
        meta: ['0.6875rem', { lineHeight: '1.4', letterSpacing: '0.14em' }],
      },
      borderRadius: {
        card: '14px',
        pill: '999px',
      },
      boxShadow: {
        lamp: '0 0 0 1px rgba(245,169,71,0.14), 0 8px 40px -12px rgba(245,169,71,0.18)',
        raise: '0 1px 0 rgba(255,255,255,0.04) inset, 0 16px 40px -24px rgba(0,0,0,0.8)',
        thread: '0 0 12px rgba(245,169,71,0.45)',
      },
      transitionTimingFunction: {
        out: 'cubic-bezier(0.22, 1, 0.36, 1)',
        swift: 'cubic-bezier(0.65, 0, 0.35, 1)',
      },
      transitionDuration: {
        DEFAULT: '240ms',
      },
      backdropBlur: {
        study: '18px',
      },
      zIndex: {
        canvas: '0',
        page: '10',
        nav: '40',
        overlay: '50',
      },
      keyframes: {
        shimmer: {
          '0%': { backgroundPosition: '-200% 0' },
          '100%': { backgroundPosition: '200% 0' },
        },
        breathe: {
          '0%, 100%': { opacity: '0.45' },
          '50%': { opacity: '1' },
        },
      },
      animation: {
        shimmer: 'shimmer 2.4s linear infinite',
        breathe: 'breathe 2.2s ease-in-out infinite',
      },
    },
  },
  plugins: [],
} satisfies Config
