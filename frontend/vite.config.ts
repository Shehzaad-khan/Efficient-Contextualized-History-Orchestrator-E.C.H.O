import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
  },
  build: {
    // Split the animation stack out of the main bundle — pages interact before
    // ambient motion needs to be ready.
    rollupOptions: {
      output: {
        manualChunks: {
          motion: ['framer-motion', 'gsap', 'lenis'],
        },
      },
    },
  },
})
