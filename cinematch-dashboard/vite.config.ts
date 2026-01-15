import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,  // true è più affidabile di '0.0.0.0' in Docker
    port: 5173,
    watch: {
      usePolling: true  // Necessario per Docker
    }
  }
})