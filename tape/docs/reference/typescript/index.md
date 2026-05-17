# TypeScript — `tape-ts`

```bash
npm install tape-ts
```

```ts
import { durableApp, outboxTool } from 'tape-ts';

const app = durableApp({
  name: 'treasury',
  budget: { usdCap: 50, tokenCap: 2_000_000 },
});
```

The full package reference is generated at build time from TSDoc via
[`typedoc-plugin-markdown`](https://typedoc-plugin-markdown.org/):

- [**Package reference**](api.md)
