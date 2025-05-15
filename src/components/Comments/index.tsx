import React from "react";
import Giscus from "@giscus/react";
import { useColorMode } from "@docusaurus/theme-common";

export default function Comments(): JSX.Element {
  const { colorMode } = useColorMode();
/**
 * <script src="https://giscus.app/client.js"
        data-repo="xwqsean/xwqsean.github.io"
        data-repo-id="R_kgDON9ChCQ"
        data-category="Announcements"
        data-category-id="DIC_kwDON9ChCc4CpVkL"
        data-mapping="pathname"
        data-strict="0"
        data-reactions-enabled="1"
        data-emit-metadata="0"
        data-input-position="bottom"
        data-theme="preferred_color_scheme"
        data-lang="zh-CN"
        crossorigin="anonymous"
        async>
</script>
 */
  return (
    <div>
      <Giscus
        id="comments"
        repo="xwqsean/xwqsean.github.io"
        repoId="R_kgDON9ChCQ"
        category="Announcements"
        categoryId="DIC_kwDON9ChCc4CpVkL"
        mapping="pathname"
        strict="0"
        reactionsEnabled="1"
        emitMetadata="0"
        inputPosition="bottom"
        theme="preferred_color_scheme"
        lang="zh-CN"
        loading="lazy"
      />
    </div>
  );
}