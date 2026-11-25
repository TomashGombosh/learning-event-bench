/**
 * Static size of each transport adapter: LOC excluding tests, comments and
 * blank lines, plus ESLint cyclomatic complexity of every function.
 *
 * Redis Pub/Sub for ShareDB is the npm package sharedb-redis-pubsub. It is
 * not in this tree, so it is reported as an external adapter with no LOC here.
 */
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Linter } from 'eslint';
import tsParser from '@typescript-eslint/parser';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const adapters = join(root, 'src', 'adapters');

function walk(dir) {
  const files = [];
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) files.push(...walk(path));
    else if (name.endsWith('.ts') && !name.endsWith('.test.ts')) files.push(path);
  }
  return files;
}

function locOf(text) {
  let loc = 0;
  let block = false;
  for (const raw of text.split('\n')) {
    let line = raw.trim();
    if (block) {
      const end = line.indexOf('*/');
      if (end < 0) continue;
      line = line.slice(end + 2).trim();
      block = false;
    }
    if (!line) continue;
    if (line.startsWith('//') || line.startsWith('*') || line.startsWith('*/')) continue;
    if (line.startsWith('/*')) {
      const end = line.indexOf('*/');
      if (end < 0) {
        block = true;
        continue;
      }
      line = line.slice(end + 2).trim();
      if (!line || line.startsWith('//')) continue;
    }
    loc += 1;
  }
  return loc;
}

function complexityOf(text, filename) {
  const linter = new Linter({ configType: 'flat' });
  const messages = linter.verify(text, [{
    files: ['**/*.ts'],
    languageOptions: { parser: tsParser, sourceType: 'module', ecmaVersion: 2022 },
    rules: { complexity: ['warn', 0] },
  }], { filename });
  const fatal = messages.filter((message) => message.fatal);
  if (fatal.length) {
    const first = fatal[0];
    throw new Error(`${filename}:${first.line} ${first.message}`);
  }
  return messages
    .filter((message) => message.ruleId === 'complexity')
    .map((message) => {
      const match = /complexity of (\d+)/.exec(message.message);
      return match ? Number(match[1]) : 0;
    });
}

function pad(value, width) {
  return String(value).padStart(width);
}

const groups = new Map();
for (const file of walk(adapters)) {
  const rel = relative(adapters, file);
  const group = rel.includes('/') ? rel.split('/')[0] : 'wiring';
  const text = readFileSync(file, 'utf8');
  const complexities = complexityOf(text, rel);
  const bucket = groups.get(group) ?? { files: 0, loc: 0, functions: 0, max: 0, sum: 0 };
  bucket.files += 1;
  bucket.loc += locOf(text);
  bucket.functions += complexities.length;
  bucket.sum += complexities.reduce((sum, value) => sum + value, 0);
  bucket.max = Math.max(bucket.max, ...complexities, 0);
  groups.set(group, bucket);
}

const names = [...groups.keys()].sort();
console.log('adapter            files   loc  fns  max_cc  sum_cc');
for (const name of names) {
  const row = groups.get(name);
  console.log(
    `${name.padEnd(16)} ${pad(row.files, 5)} ${pad(row.loc, 5)} ${pad(row.functions, 4)} ${pad(row.max, 7)} ${pad(row.sum, 7)}`,
  );
}
console.log('sharedb-redis-pubsub  external npm package (not counted above)');
if (!names.includes('sharedb') || !names.includes('kafka') || !names.includes('redis-streams')) {
  console.error('expected sharedb, kafka and redis-streams adapters');
  process.exit(1);
}
