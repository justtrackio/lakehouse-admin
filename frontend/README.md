# Lakehouse Admin Frontend

A modern frontend application built with TypeScript, Bun, Vite, React, TanStack Query, TanStack Router, and Ant Design.

## Tech Stack

- **TypeScript** - Type-safe JavaScript
- **Bun** - Fast JavaScript runtime and package manager
- **Vite** - Lightning-fast build tool
- **React** - UI library
- **TanStack Query** - Powerful data fetching and caching
- **TanStack Router** - Type-safe routing with file-based routing
- **Ant Design** - Enterprise-class UI design system

## Getting Started

### Prerequisites

- Bun installed (or Node.js 18+ with npm/pnpm)

### Installation

```bash
bun install
```

### Development

Start the development server:

```bash
bun run dev
```

The application will be available at `http://localhost:5173`

**Note:** On first run, TanStack Router will generate `src/routeTree.gen.ts`. If your IDE shows TypeScript errors in route files, restart your TypeScript server (in VS Code: `Cmd/Ctrl+Shift+P` → "TypeScript: Restart TS Server").

### Building

Build for production:

```bash
bun run build
```

### Linting

Run ESLint:

```bash
bun run lint
```

### Preview Production Build

Preview the production build locally:

```bash
bun run preview
```

## Project Structure

```
frontend/
├── src/
│   ├── api/          # API client and configurations
│   ├── components/   # Reusable React components
│   ├── hooks/        # Custom React hooks
│   ├── routes/       # TanStack Router file-based routes
│   │   ├── __root.tsx    # Root layout component
│   │   ├── index.tsx     # Home page (/)
│   │   └── about.tsx     # About page (/about)
│   ├── main.tsx      # Application entry point
│   └── index.css     # Global styles
├── public/           # Static assets
└── package.json
```

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```env
VITE_API_BASE_URL=http://localhost:8080
```

### API Client

The API client is centralized in `src/api/client.ts`. Use it for all HTTP requests:

```typescript
import { apiClient } from '@/api/client';

// Example GET request
const data = await apiClient.get('/endpoint');

// Example POST request
const result = await apiClient.post('/endpoint', { data });
```

### Routing

Routes are defined using TanStack Router's file-based routing system. Create new route files in `src/routes/`:

- `src/routes/index.tsx` → `/`
- `src/routes/about.tsx` → `/about`
- `src/routes/users/index.tsx` → `/users`
- `src/routes/users/$id.tsx` → `/users/:id`

### Data Fetching

Use TanStack Query for data fetching:

```typescript
import { useQuery } from '@tanstack/react-query';
import { apiClient } from '@/api/client';

function MyComponent() {
  const { data, isLoading, error } = useQuery({
    queryKey: ['myData'],
    queryFn: () => apiClient.get('/api/data'),
  });

  if (isLoading) return <Spin />;
  if (error) return <Alert type="error" message="Error loading data" />;

  return <div>{/* render data */}</div>;
}
```

## Code Style

- TypeScript strict mode enabled
- Follow existing patterns in the codebase
- Use functional components with hooks
- Prefer explicit types over `any`
- Keep components focused and reusable

## Development Tools

- **TanStack Router Devtools** - Available in development mode for debugging routes
- **ESLint** - Code linting with TypeScript support
- **Hot Module Replacement (HMR)** - Instant updates during development

