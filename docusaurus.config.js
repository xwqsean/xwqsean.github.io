// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import {themes as prismThemes} from 'prism-react-renderer';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: '破晓',
  tagline: "破晓轻染仙云影，幽梦随风落琼花。碧霄飘渺凝仙韵，日出金辉幻瑶霞。",
  favicon: 'img/mylogo.png',

  // Set the production url of your site here
  url: 'https://xwqsean.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'xwqsean', // Usually your GitHub org/user name.
  projectName: 'xwqsean.github.io', // Usually your repo name.
  trailingSlash: false,
  deploymentBranch: 'dev',

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'zh-Hans',
    locales: ['zh-Hans'],
  },

  // SEO: inject Google & Baidu site-verification meta tags into <head>
  headTags: [
    {
      tagName: 'meta',
      attributes: {
        name: 'google-site-verification',
        content: '2cb0d93a21462648',
      },
    },
    // TODO: Replace the content value below with your Baidu verification code
    // obtained from https://ziyuan.baidu.com/  (百度搜索资源平台 -> 网站验证)
    // {
    //   tagName: 'meta',
    //   attributes: {
    //     name: 'baidu-site-verification',
    //     content: 'YOUR_BAIDU_VERIFICATION_CODE',
    //   },
    // },
  ],

  // SEO: Baidu auto-push script (百度自动推送) — speeds up Baidu indexing of new pages.
  // Loaded from Baidu's official CDN (bdstatic.com). SRI is not applied because
  // this CDN script is updated by Baidu and a pinned hash would break on updates.
  scripts: [
    {
      src: 'https://zz.bdstatic.com/linksubmit/push.js',
      defer: true,
    },
  ],


  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          // Please change this to your repo.
          // // Remove this to remove the "edit this page" links.
          // editUrl:
          //   'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
        },
        blog: {
          showReadingTime: true,
          feedOptions: {
            type: ['rss', 'atom'],
            xslt: true,
          },
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          // editUrl:
          //   'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
          // Useful options to enforce blogging best practices
          onInlineTags: 'warn',
          onInlineAuthors: 'warn',
          onUntruncatedBlogPosts: 'warn',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
        sitemap: {
          lastmod: 'date',
          ignorePatterns: ['/tags/**', '/blog/tags/**'],
          filename: 'sitemap.xml',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: 'img/docusaurus-social-card.jpg',
      navbar: {
        title: '强哥叨逼叨',
        logo: {
          alt: 'Logo',
          src: 'img/mylogo.png',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'tutorialSidebar',
            position: 'left',
            label: '文档',
          },
          {to: '/blog', label: '博客', position: 'left'},
          {to: '/about', label: '关于我', position: 'left'},
          {
            href: 'https://xwqsean.github.io/Paper.html',
            label: 'markdown',
            position: 'right',
          },
          {
            href: 'https://github.com/xwqsean',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        logo: {
          alt: 'Meta 开源图标',
          src: 'img/wechat.jpg',
          href: 'https://mp.weixin.qq.com/s?__biz=MzI0MDEzODc5MA==&mid=2247484604&idx=1&sn=87e3ff14d0078396c0cb653c2b9ae983&chksm=e91e2bf5de69a2e3cf86ea14b2ab7bf80dbaaef09d0ea2267d2390fc2e7f0e0aaf08d07864e2&token=1661169767&lang=zh_CN#rd',
          width: 65,
          height: 65,
        },
        links: [
          // {
          //   title: 'Docs',
          //   items: [
          //     {
          //       label: 'Docs',
          //       to: '/docs/intro',
          //     },
          //   ],
          // },
          // {
          //   title: 'Community',
          //   items: [
          //     {
          //       label: 'Stack Overflow',
          //       href: 'https://stackoverflow.com/questions/tagged/docusaurus',
          //     },
          //   ],
          // },
          // {
          //   title: 'More',
          //   items: [
          //     {
          //       label: 'Blog',
          //       to: '/blog',
          //     },
          //     {
          //       label: 'GitHub',
          //       href: 'https://github.com/xwqsean',
          //     },
          //   ],
          // },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} 破晓`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['powershell'],
      },
    }),
};

export default config;
