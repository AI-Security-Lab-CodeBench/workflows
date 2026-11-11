const path = require('node:path');
const upstream = require('./.upstream/js_modules/app-oss/next.config.js');

module.exports = (phase) => {
  const inherited = upstream(phase);
  return {
    ...inherited,
    assetPrefix: '',
    webpack(config, context) {
      const result = inherited.webpack(config, context);
      result.cache = false;
      result.resolve.alias = {
        ...result.resolve.alias,
        '@shared/assets/AssetsOverviewRoot$': path.resolve(__dirname, 'src/AssetsOverviewRoot.tsx'),
        'zod$': path.resolve(__dirname, 'tooling/node_modules/zod'),
      };
      return result;
    },
  };
};
