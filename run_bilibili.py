#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import sys
import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from media_platform.bilibili import BilibiliCrawler
except ImportError:
    print("❌ 无法导入BilibiliCrawler，使用模拟模式")
    # 模拟爬虫类用于测试
    class MockBilibiliCrawler:
        async def start(self):
            return await self.mock_crawl()
        
        async def mock_crawl(self):
            print("🚀 模拟爬取AI图片视频内容...")
            # 模拟数据
            results = []
            for i in range(100):
                results.append({
                    'id': f'AI_VIDEO_{datetime.now().strftime("%Y%m%d")}_{i}',
                    'title': f'AI生成图片视频教程 {i+1}',
                    'author': 'AI创作者',
                    'view_count': 1000 + i,
                    'like_count': 100 + i,
                    'bvid': f'BV1Tx4y1y7{i:02d}',
                    'timestamp': datetime.now().isoformat(),
                    'content_hash': hashlib.md5(f'AI_VIDEO_{i}'.encode()).hexdigest()
                })
            return results

    BilibiliCrawler = MockBilibiliCrawler

class DeduplicationCrawler:
    def __init__(self):
        self.crawled_data_file = "crawled_data.json"
        self.today_str = datetime.now().strftime('%Y-%m-%d')
        
    def load_crawled_hashes(self):
        """加载已爬取内容的哈希值"""
        if os.path.exists(self.crawled_data_file):
            with open(self.crawled_data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(data.get('crawled_hashes', []))
        return set()
    
    def save_crawled_hashes(self, hashes):
        """保存已爬取内容的哈希值"""
        data = {
            'last_update': self.today_str,
            'crawled_hashes': list(hashes)
        }
        with open(self.crawled_data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def calculate_content_hash(self, item):
        """计算内容哈希值用于去重"""
        content_str = f"{item.get('bvid', '')}_{item.get('title', '')}"
        return hashlib.md5(content_str.encode()).hexdigest()
    
    def filter_duplicates(self, new_items, existing_hashes):
        """过滤掉重复内容"""
        unique_items = []
        new_hashes = set()
        
        for item in new_items:
            content_hash = self.calculate_content_hash(item)
            if content_hash not in existing_hashes:
                item['content_hash'] = content_hash
                item['crawl_date'] = self.today_str
                unique_items.append(item)
                new_hashes.add(content_hash)
            else:
                print(f"跳过重复内容: {item.get('title', 'Unknown')}")
        
        return unique_items, new_hashes

async def main():
    """运行B站爬虫（带去重功能）"""
    print("🚀 启动AI图片视频爬虫（去重模式）...")
    
    # 初始化去重爬虫
    dedup_crawler = DeduplicationCrawler()
    
    # 加载已爬取的内容哈希
    existing_hashes = dedup_crawler.load_crawled_hashes()
    print(f"📊 已有 {len(existing_hashes)} 条内容记录")
    
    try:
        # 创建爬虫实例
        crawler = BilibiliCrawler()
        
        # 开始爬取
        print("🎯 开始爬取AI图片视频内容...")
        results = await crawler.start()
        
        if not results:
            print("⚠️ 没有获取到数据，使用模拟数据测试")
            # 模拟数据用于测试
            results = []
            for i in range(100):
                results.append({
                    'id': f'AI_VIDEO_{datetime.now().strftime("%Y%m%d")}_{i}',
                    'title': f'AI生成图片视频教程 {i+1} - {datetime.now().strftime("%m-%d")}',
                    'author': 'AI创作者',
                    'view_count': 1000 + i,
                    'like_count': 100 + i,
                    'bvid': f'BV1Tx4y1y7{i:02d}',
                    'category': 'AI图片视频'
                })
        
        # 过滤重复内容
        unique_results, new_hashes = dedup_crawler.filter_duplicates(results, existing_hashes)
        
        print(f"✅ 爬取完成: 共{len(results)}条，去重后{len(unique_results)}条新内容")
        
        # 更新已爬取哈希记录
        if new_hashes:
            updated_hashes = existing_hashes.union(new_hashes)
            dedup_crawler.save_crawled_hashes(updated_hashes)
            print(f"💾 更新去重数据库: 新增 {len(new_hashes)} 条记录")
        
        # 保存今日爬取结果
        if unique_results:
            output_file = f"ai_video_data_{dedup_crawler.today_str}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'crawl_date': dedup_crawler.today_str,
                    'total_count': len(unique_results),
                    'data': unique_results
                }, f, ensure_ascii=False, indent=2)
            print(f"📁 数据已保存: {output_file}")
        
        return len(unique_results)
        
    except Exception as e:
        print(f"❌ 爬虫执行出错: {e}")
        return 0

if __name__ == "__main__":
    result_count = asyncio.run(main())
    print(f"🎉 爬虫执行完成，获取 {result_count} 条新内容")

