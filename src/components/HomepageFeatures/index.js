import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: '何地',
    imgUrl: '/img/4.jpg',
    description: (
      <>
        一个半吊子程序员技术博客和知识教程的分享地，写分享只是为了让自己显的比较积极。
        虽然内心燃烧着向上的火苗，但火势却不怎么猛。
      </>
    ),
  },
  {
    title: '喜恶',
    imgUrl: '/img/5.jpg',
    description: (
      <>
        喜欢篮球，只看小皇帝。读书时只有篮球和学习，近些年才开始痴迷玄幻修仙：《遮天》、《斗破苍穹》、《百炼成神》已读，《剑来》进行中。
        不喜社交，比较单调。想改，又难……
      </>
    ),
  },
  {
    title: '找我',
    imgUrl: '/img/6.jpg',
    description: (
      <>
        对我感兴趣？去微信公众号“强哥叨逼叨”找我聊天
        不关注别来。
      </>
    ),
  },
];

function Feature({imgUrl, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <img src={imgUrl} role="img" style={{width: 360+'px',height: 410 +'px'}} />
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
