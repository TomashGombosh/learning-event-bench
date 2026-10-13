import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

const root = join(__dirname, '../../src');

function filesIn(dir: string): string[] {
  return readdirSync(dir).flatMap((entry) => {
    const path = join(dir, entry);
    return statSync(path).isDirectory() ? filesIn(path) : path.endsWith('.ts') ? [path] : [];
  });
}

const importsOf = (file: string): string[] =>
  [...readFileSync(file, 'utf8').matchAll(/from\s+'([^']+)'/g)].map((m) => m[1]);

/** Clean Architecture dependency rule: source code dependencies point only inwards. */
describe('dependency rule', () => {
  it('domain depends on nothing outside the domain', () => {
    for (const file of filesIn(join(root, 'domain'))) {
      for (const dep of importsOf(file)) expect(dep.startsWith('.') || dep.startsWith('node:')).toBe(true);
    }
  });

  it('application depends only on domain (no frameworks, no adapters)', () => {
    for (const file of filesIn(join(root, 'application'))) {
      for (const dep of importsOf(file)) {
        expect(dep.startsWith('@domain/') || dep.startsWith('.')).toBe(true);
      }
    }
  });

  it('adapters never import from apps', () => {
    for (const file of filesIn(join(root, 'adapters'))) {
      for (const dep of importsOf(file)) expect(dep.includes('apps/')).toBe(false);
    }
  });
});
