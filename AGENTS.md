# AGENT GUIDELINES

1. Backend (Go): build with `cd backend && go build ./...`.
2. Backend tests: `cd backend && go test ./...`; single test: `cd backend && go test ./... -run TestName`.
3. Backend style: run `cd backend && go fmt ./...`; standard library imports first, then external, grouped and alphabetized.
4. Backend naming: exported identifiers use PascalCase, locals use camelCase; avoid unnecessary abbreviations.
5. Backend errors: never ignore `err`; wrap with `fmt.Errorf("context: %w", err)` as in handlers and return early.
6. Spark / Trino clients: extend existing clients in `backend/spark_client.go` and `backend/trino_client.go` instead of creating new ones.
7. Frontend (Vite/TypeScript): install deps with `cd frontend && bun install` (or `npm install`).
8. Frontend dev server: `cd frontend && bun run dev`.
9. Frontend build: `cd frontend && bun run build`.
10. Frontend lint: `cd frontend && bun run lint` (ESLint flat config in `frontend/eslint.config.js`).
11. Frontend tests: none configured yet; prefer adding Vitest in `frontend` if tests are introduced.
12. Frontend style: TypeScript strict mode via `typescript-eslint`; avoid `any`, prefer explicit types and interfaces.
13. React components: function components with PascalCase names; keep hooks at the top of components and follow `TablesPage` patterns.
14. Routing: use TanStack Router’s generated route tree (`routeTree.gen.ts`) and configure routes via files in `frontend/src/routes`.
15. Data fetching: prefer `@tanstack/react-query` with descriptive `queryKey`s; centralize HTTP in `frontend/src/api/client.ts`.
16. Error and loading UI: follow `TablesPage` using `Spin` and `Alert` from `antd` for consistent UX.
17. Formatting: rely on project tooling only (Go formatter, ESLint/TypeScript, Vite); keep semicolons and quotes consistent with existing code.
18. Do not add new global tooling, linters, or formatters without explicit instruction; work within the current stack.
19. There are currently no Cursor rules (`.cursor/rules` or `.cursorrules`) or GitHub Copilot instructions (`.github/copilot-instructions.md`).
20. If such tools are introduced, update this file to reference and align with their repository-specific guidelines.
