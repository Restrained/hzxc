import subprocess
import time
import argparse
import json


def run_spider_in_screen(task_name, concurrency, queue_name, init_queue_size):
    """
    启动爬虫任务在一个独立的 screen 窗口中
    """
    # 定义屏幕会话名
    screen_session_name = f"spider_task_{task_name}"

    # 通过命令行启动爬虫任务
    command = [
        'screen', '-dmS', screen_session_name,  # 启动后台屏幕会话
        'bash', '-c',  # 执行bash命令
        f'workon bricks && python3 -c "from events.spiders.rhhz.modules.article_incremental import ArticleIncrementalCrawler; '
        f'spider = ArticleIncrementalCrawler(concurrency={concurrency}, '
        f'init_queue_size={init_queue_size}, '
        f'queue_name=\'{queue_name}\', '
        f'spider.run(task_name=\'{task_name}\')"'
    ]

    # 使用 subprocess 启动screen会话并执行任务
    print("1111111")
    print(' '.join(command))
    subprocess.run(command, check=True)
    print(f"爬虫任务已启动，在独立的 screen 会话中运行，任务名: {task_name}, 并发数: {concurrency}, 队列名: {queue_name}")


def load_config(config_file):
    """
    从配置文件加载配置
    """
    with open(config_file, 'r') as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="Start the spider with specific parameters in a screen session.")

    # 通过命令行传递配置文件路径
    parser.add_argument("--config", help="Path to config file", required=True)
    parser.add_argument("--task_name", help="Override task name", default=None)
    parser.add_argument("--concurrency", type=int, help="Override concurrency", default=None)
    parser.add_argument("--queue_name", help="Override queue name", default=None)
    parser.add_argument("--init_queue_size", type=int, help="Override init_queue_size", default=None)

    # 解析命令行参数
    args = parser.parse_args()

    # 加载配置文件
    config = load_config(args.config)

    # 从配置文件中获取参数，命令行参数可以覆盖配置文件中的值
    task_name = args.task_name or config.get("task_name", "all")
    concurrency = args.concurrency or config.get("concurrency", 1)
    queue_name = args.queue_name or config.get("queue_name", "article_incremental")
    init_queue_size = args.init_queue_size or config.get("init_queue_size", 1000000)

    # 启动爬虫任务
    run_spider_in_screen(task_name, concurrency, queue_name, init_queue_size)


if __name__ == "__main__":
    main()
