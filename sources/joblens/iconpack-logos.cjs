const { icons, width, height } = require('@iconify/json/json/logos.json');
const needed = [
  'spring-icon', 'docker-icon', 'github-actions',
  'postgresql', 'redis', 'elasticsearch',
  'prometheus', 'grafana', 'aws-ec2',
];
const filtered = {};
for (const k of needed) if (icons[k]) filtered[k] = icons[k];
module.exports = { prefix: 'logos', width: width ?? 24, height: height ?? 24, icons: filtered };
