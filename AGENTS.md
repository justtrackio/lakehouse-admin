# AGENT GUIDELINES

This document is the **authoritative source of truth** for AI agents (and humans) working in the `lakehouse-admin` repository.
Strict adherence to these guidelines is required to maintain the stability and quality of the codebase.

## 1. Project Context & Architecture

The `lakehouse-admin` project is a management interface for a Data Lakehouse, providing tools to browse metadata, optimize tables (Iceberg), and manage maintenance tasks.

*   **Backend (`/backend`)**:
    *   **Language**: Go 1.25+
    *   **Framework**: `gosoline` (JustTrack's opinionated framework).
    *   **Role**: Serves the REST API, handles background jobs, and communicates with Trino/Spark.
    *   **Data Access**: Uses `sqlx` and `gosoline`'s db helpers.
*   **Frontend (`/frontend`)**:
    *   **Framework**: React 19 (Vite) + TypeScript.
    *   **UI Library**: Ant Design (`antd`).
    *   **State/Data**: `@tanstack/react-query` for server state.
    *   **Routing**: `@tanstack/react-router` (File-based routing).
*   **Infrastructure**:
    *   **Database**: MySQL (Metadata), Trino (Query Engine).
    *   **Deployment**: Docker containers.

## 2. Development Environment & Commands

### Backend (`/backend`)

Always run commands from the `backend` directory.

*   **Build**:
    ```bash
    go build ./...
    ```
    *   *Rule*: Run this after *any* backend change to ensure type safety.

*   **Testing**:
    *   **Run All Tests**:
        ```bash
        go test ./...
        ```
    *   **Run Single Test**:
        ```bash
        go test ./... -run TestName
        # Example: go test ./... -run TestOptimizeTable
        ```
    *   **Verbose Output** (Useful for debugging):
        ```bash
        go test -v ./...
        ```

*   **Code Quality**:
    *   **Format**: `go fmt ./...` (Mandatory before committing)
    *   **Dependency Management**: `go mod tidy`

### Frontend (`/frontend`)

Always run commands from the `frontend` directory. We prefer `bun`, but `npm` is acceptable.

*   **Install Dependencies**:
    ```bash
    bun install
    ```

*   **Development Server**:
    ```bash
    bun run dev
    ```

*   **Build & Type Check**:
    ```bash
    bun run build
    ```
    *   *Rule*: This runs `tsc -b`. Run this after *any* frontend change to verify type safety.

*   **Linting**:
    ```bash
    bun run lint
    ```
    *   *Rule*: Fix all linting errors before finishing a task.

*   **Testing**:
    *   *Status*: No frontend tests are currently configured. Do not attempt to run `bun test`.

## 3. Code Style & Conventions

### Backend (Go)

*   **Naming**:
    *   **Exported**: `PascalCase` (e.g., `TableService`).
    *   **Unexported**: `camelCase` (e.g., `tableService`).
    *   **Acronyms**: Keep them consistent (e.g., `ServeHTTP`, `ID`, `URL` - NOT `ServeHttp`, `Id`, `Url`).
    *   **Interfaces**: Suffix with `-er` where applicable (e.g., `Reader`, `Writer`).

*   **Error Handling**:
    *   **Strictness**: Never ignore errors (`_`). Handle them or return them.
    *   **Context**: Always wrap errors when bubbling up:
        ```go
        return fmt.Errorf("failed to fetch metadata for table %s: %w", tableName, err)
        ```
    *   **Guard Clauses**: Return early to avoid deep nesting.

*   **Types & JSON**:
    *   **DateTime**: Use the custom `DateTime` type found in `backend/datetime.go` for all fields that need to serialize to JSON dates. This ensures consistent RFC3339 formatting.
    *   **DTOs**: specific request/response structs should be defined in `types.go` or specific modules, not inline.

### Frontend (TypeScript/React)

*   **Strict Mode**: `noImplicitAny` is ON. You must define types.
*   **Naming**:
    *   **Components**: `PascalCase.tsx` (e.g., `TableList.tsx`).
    *   **Hooks**: `camelCase.ts` (e.g., `useTableData.ts`).
    *   **Variables**: `camelCase`.

*   **Component Structure**:
    *   Use **Functional Components** only.
    *   Keep components small and focused. Extract sub-components if a file exceeds ~200 lines.
    *   Use `antd` components for all UI elements to maintain consistency.

*   **Data Fetching**:
    *   **Queries**: Use `useQuery` for GET requests.
    *   **Mutations**: Use `useMutation` for POST/PUT/DELETE requests.
    *   **API Client**: All API calls must go through `src/api/client.ts`. Define types in `src/api/schema.ts`.

## 4. Implementation Guidelines

### Routing (TanStack Router)
*   Routes are generated based on the file structure in `frontend/src/routes`.
*   **Creating a Route**:
    1.  Create a file like `frontend/src/routes/tables/index.tsx`.
    2.  Export the component using `createFileRoute`.
    3.  **Do not edit** `routeTree.gen.ts` manually. It usually regenerates on dev server start, but if you add a file, you might need to restart the dev server.

### Adding an API Endpoint
1.  **Backend**:
    *   Define the handler in `backend/handler_*.go`.
    *   Register the route in `backend/main.go` (or wherever the module definition is).
    *   Ensure proper error handling and logging.
2.  **Frontend**:
    *   Add the request and response interface in `frontend/src/api/schema.ts`.
    *   Add the fetch function in `frontend/src/api/client.ts`.
    *   Create a custom hook (e.g., `useMyData`) using `useQuery` or `useMutation` that calls the client function.

## 5. Agent Operational Rules

These rules are designed to prevent common mistakes made by LLMs.

1.  **Read Before Write**: Always `read` a file before `edit`ing it. Context is key.
2.  **Verify After Write**:
    *   If you changed Go code -> Run `go build ./...` in `/backend`.
    *   If you changed TS code -> Run `bun run build` in `/frontend`.
3.  **No Blind Fixes**: If a build fails, read the error message carefully. Do not just guess a fix. Analyze the root cause.
4.  **Respect Conventions**: Do not introduce new libraries (e.g., `axios`, `moment`, `lodash`) if standard ones or existing project dependencies can do the job.
    *   Use `dayjs` (if already present) or native `Date` for JS dates.
    *   Use `fetch` (via the existing client wrapper) for HTTP.
5.  **Incremental Changes**: Do not rewrite an entire file to change one function. Use targeted edits.
6.  **Tooling**: Use the provided tools. Do not try to install `prettier` or `eslint` globally; use the project's scripts.

## 6. External Rules
*   No `.cursor/rules` or `.github/copilot-instructions.md` exist. This file is the sole guide.
