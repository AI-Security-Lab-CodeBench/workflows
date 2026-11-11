const path = require('node:path');
const application = path.resolve(__dirname, '.upstream/js_modules/app-oss');
const inherited = require(path.join(application, 'postcss.config.js'));

module.exports = {
  ...inherited,
  plugins: Object.fromEntries(
    Object.entries(inherited.plugins).map(([name, options]) => [
      require.resolve(name, {paths: [application]}),
      options,
    ]),
  ),
};
