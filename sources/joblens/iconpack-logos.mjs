import { readFileSync } from 'fs';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const raw = require('@iconify/json/json/logos.json');

const needed = [
  'spring-icon', 'docker-icon', 'github-actions',
  'postgresql', 'redis', 'elasticsearch',
  'prometheus', 'grafana', 'aws-ec2', 'java'
];

const icons = {};
for (const name of needed) {
  if (raw.icons[name]) icons[name] = raw.icons[name];
}

export default {
  prefix: 'logos',
  width:  raw.width  ?? 24,
  height: raw.height ?? 24,
  icons,
};
