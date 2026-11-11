import { spawn } from 'node:child_process'

const server = process.argv[2]
if (server !== 'agent' && server !== 'operator') {
  throw new Error(`Unknown dev server: ${server}`)
}

const child = spawn('tsx', ['watch', `server/${server}.ts`], {
  env: { ...process.env, INNGEST_DEV: '1' },
  stdio: 'inherit',
  shell: process.platform === 'win32',
})

child.on('error', (error) => {
  console.error(`Failed to start ${server} dev server:`, error)
  process.exitCode = 1
})

child.on('exit', (code, signal) => {
  process.exitCode = code ?? (signal ? 1 : 0)
})
