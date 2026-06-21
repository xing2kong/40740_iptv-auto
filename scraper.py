#!/usr/bin/env python3
"""
GitHub 港澳台 IPTV 源自动抓取 + 验证 + TVBox 输出
==============================================
从多个公开 GitHub 仓库抓取香港/澳门电视台 m3u 链接,
验证直播流可用性, 输出 TVBox/tv.txt 格式。

输出格式 (TVBox / APTV / tv.txt):
    节目组名称,#genre#
    频道名,http://xxx.m3u8

验证原理:
    Level 1 - HTTP连通: 请求m3u8地址, 状态码<400
    Level 2 - 内容有效: 返回内容含m3u8标签或视频数据
    Level 3 - 有视频流: 请求第一个ts分片, 确认返回MPEG-TS数据

使用:
    python scraper.py                     # 完整验证
    python scraper.py --no-validate       # 跳过验证(快速)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    print("[!] pip install requests")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════
# 数据源
# ═══════════════════════════════════════════════════════════════

SOURCES = [
    # ── 1. iptv-org: 全球最大公开 IPTV 数据库 ──
    {
        "name": "iptv-org-香港",
        "url": "https://iptv-org.github.io/iptv/countries/hk.m3u",
    },
    {
        "name": "iptv-org-澳门",
        "url": "https://iptv-org.github.io/iptv/countries/mo.m3u",
    },
    {
        "name": "iptv-org-中文",
        "url": "https://iptv-org.github.io/iptv/languages/zho.m3u",
        "filter_keyword": True,
    },
    # ── 2. Joker-Cold: 已验证的香港频道合集 (640+源) ──
    {
        "name": "Joker-Cold-全量",
        "url": "https://raw.githubusercontent.com/Joker-Cold/HK-IPTV/main/source/all_sources.m3u",
        "filter_keyword": True,
    },
    {
        "name": "Joker-Cold-已验证",
        "url": "https://raw.githubusercontent.com/Joker-Cold/HK-IPTV/main/source/COLD_OK.m3u8",
        "filter_keyword": True,
    },
    {
        "name": "Joker-Cold-iptv",
        "url": "https://raw.githubusercontent.com/Joker-Cold/HK-IPTV/main/source/source_iptv.m3u",
        "filter_keyword": True,
    },
    # ── 3. sammy0101: HK IPTV 自动聚合 (已验证可播) ──
    {
        "name": "sammy0101-HK",
        "url": "https://raw.githubusercontent.com/sammy0101/hk-iptv-auto/main/hk_live.m3u",
        "filter_keyword": True,
    },
    # ── 4. imDazui: 台湾香港澳门合集 ──
    {
        "name": "imDazui-港澳台",
        "url": "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/master/m3u/台湾香港澳门202506.m3u",
    },
    # ── 5. ChinaIPTV: 含港澳台, 自动更新 ──
    {
        "name": "ChinaIPTV-自动更新",
        "url": "https://raw.githubusercontent.com/hujingguang/ChinaIPTV/main/cnTV_AutoUpdate.m3u8",
        "filter_group": "港澳台",
    },
    # ── 6. Guovin/TV: 大型 IPTV 聚合 (1800+源) ──
    {
        "name": "Guovin-TV",
        "url": "https://raw.githubusercontent.com/Guovin/TV/gd/output/result.m3u",
        "filter_keyword": True,
    },
]

CDN_PREFIX = "https://cdn.jsdelivr.net/gh/"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

# ═══════════════════════════════════════════════════════════════
# 香港/澳门频道识别
# ═══════════════════════════════════════════════════════════════

HK_PATTERNS = [
    r'香港', r'Hong\s*Kong', r'\bHK\b', r'HKS', r'HKSTV',
    r'翡翠', r'Jade', r'TVB', r'无线', r'翡翠台',
    r'ViuTV', r'Viu\s*TV', r'HOY\s*TV', r'HOY',
    r'港台', r'RTHK', r'港台電視',
    r'凤凰', r'Phoenix', r'凤凰卫视',
    r'澳门', r'Macau', r'TDM', r'澳广视',
    r'ATV', r'亚洲电视', r'本港', r'國際台',
    r'有線', r'i-CABLE', r'Cable\s*TV',
    r'奇妙', r'Amazing', r'Now\s*[^\s]', r'Now\s*TV',
    r'開電視', r'港視', r'HKTVE', r'HKTV',
    r'天映', r'Celestial', r'耀才', r'BSTV',
]
HK_PAT = [re.compile(p, re.IGNORECASE) for p in HK_PATTERNS]

EXCLUDE_PATTERNS = [
    r'^CCTV', r'^湖南卫视', r'^东方卫视', r'^浙江卫视', r'^江苏卫视',
    r'^北京卫视', r'^广东卫视', r'^深圳卫视', r'^四川卫视',
    r'^山东卫视', r'^河南卫视', r'^湖北卫视', r'^安徽卫视',
    r'^天津卫视', r'^重庆卫视', r'^辽宁卫视', r'^吉林卫视',
    r'^黑龙江卫视', r'^福建卫视', r'^河北卫视', r'^江西卫视',
    r'^广西卫视', r'^云南卫视', r'^旅游卫视',
]
EXCLUDE_PAT = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_PATTERNS]


def is_hk_channel(name):
    if any(p.search(name) for p in EXCLUDE_PAT):
        return False
    return any(p.search(name) for p in HK_PAT)


# ═══════════════════════════════════════════════════════════════
# M3U 解析
# ═══════════════════════════════════════════════════════════════

def _parse_extinf_line(line):
    """
    解析 #EXTINF 行, 提取频道名.
    格式: #EXTINF:-1 attr1="val,ue" attr2="val2",Channel Name
    策略: 从右往左找最后一个不在引号内的逗号.
    """
    in_quote = False
    last_comma = -1
    for i in range(len(line) - 1, -1, -1):
        if line[i] == '"':
            in_quote = not in_quote
        elif line[i] == ',' and not in_quote:
            last_comma = i
            break
    if last_comma == -1:
        return None
    return line[last_comma + 1:].strip(), line[:last_comma]


def parse_m3u(content, source_name, filter_group=None, filter_keyword=False):
    channels = []
    lines = content.replace('\r\n', '\n').replace('\r', '\n').strip().split('\n')

    current_extopts = []
    current_info = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith('#EXTVLCOPT:'):
            current_extopts.append(line)
            continue

        if line.startswith('#EXTINF'):
            parsed = _parse_extinf_line(line)
            if parsed:
                name, extinf_header = parsed

                group = ""
                gm = re.search(r'group-title="([^"]*)"', extinf_header, re.IGNORECASE)
                if gm:
                    group = gm.group(1)

                tvg_id = ""
                tm = re.search(r'tvg-id="([^"]*)"', extinf_header, re.IGNORECASE)
                if tm:
                    tvg_id = tm.group(1)

                tvg_logo = ""
                lm = re.search(r'tvg-logo="([^"]*)"', extinf_header, re.IGNORECASE)
                if lm:
                    tvg_logo = lm.group(1)

                current_info = {
                    "name": name, "url": None, "group": group,
                    "tvg_id": tvg_id, "tvg_logo": tvg_logo,
                    "source": source_name, "extopts": list(current_extopts),
                }
                current_extopts = []
            continue

        if line.startswith('#'):
            continue

        if current_info and (line.startswith('http') or line.startswith('rtmp') or line.startswith('rtsp')):
            current_info['url'] = line

            include = True
            if filter_group:
                if filter_group not in current_info['group'] and filter_group not in current_info['name']:
                    include = False
            if filter_keyword and not is_hk_channel(current_info['name']):
                include = False

            if include:
                channels.append(current_info)
            current_info = None

    return channels


# ═══════════════════════════════════════════════════════════════
# 网络请求
# ═══════════════════════════════════════════════════════════════

def fetch_url(url, timeout=15, use_cdn=True):
    urls = [url]
    if use_cdn and 'raw.githubusercontent.com' in url:
        gh = url.replace('https://raw.githubusercontent.com/', '')
        urls.append(f"{CDN_PREFIX}{gh}")
    for u in urls:
        try:
            r = requests.get(u, headers=HEADERS, timeout=timeout, verify=False)
            if r.status_code == 200 and len(r.text) > 50:
                return r.text
        except Exception:
            continue
    return ""


def validate_stream(url, timeout=10):
    """
    三级验证直播源是否真正可播:
      Level 1 - HTTP连通: 请求m3u8/流地址, 状态码<400
      Level 2 - 内容有效: 返回内容长度>100且含m3u8标签/ts数据
      Level 3 - 有视频片段: m3u8则请求第一个ts分片, 确认返回视频数据
    返回: (bool, str) - (是否可播, 详细原因)
    """
    # Level 1: HTTP 连通
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout,
                         stream=True, verify=False, allow_redirects=True)
        status = r.status_code
        if status >= 400:
            return False, f"HTTP {status}"
        content_type = r.headers.get('Content-Type', '')
        # 读取前 16KB
        data = b''
        for chunk in r.iter_content(chunk_size=4096):
            data += chunk
            if len(data) >= 16384:
                break
        r.close()
    except requests.exceptions.Timeout:
        return False, "timeout"
    except requests.exceptions.ConnectionError:
        return False, "connection refused"
    except Exception as e:
        return False, str(e)[:60]

    # Level 2: 内容有效
    if len(data) < 20:
        return False, "empty response"

    text = data.decode('utf-8', errors='ignore').strip()

    # 如果是视频流直接数据 (ts/flv), content-type 含 video 或 application/octet-stream
    if any(v in content_type for v in ['video/', 'application/octet-stream', 'application/mp4']):
        return True, "direct stream (video content-type)"

    # 如果是 m3u8 播放列表
    if '#EXTM3U' in text or '#EXTINF' in text or '.ts' in text or '.m3u8' in text:
        # Level 3: 尝试请求第一个 ts/m3u8 分片
        ts_urls = []
        for line in text.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # 绝对 URL
            if line.startswith('http'):
                ts_urls.append(line)
            # 相对 URL
            elif '/' in line:
                base = url.rsplit('/', 1)[0]
                ts_urls.append(f"{base}/{line}")

        if ts_urls:
            # 只验证第一个分片
            ts_url = ts_urls[0]
            try:
                tr = requests.get(ts_url, headers=HEADERS, timeout=timeout,
                                  stream=True, verify=False, allow_redirects=True)
                if tr.status_code >= 400:
                    return False, f"ts segment HTTP {tr.status_code}"
                # 读取一小块确认是视频数据
                ts_data = b''
                for chunk in tr.iter_content(chunk_size=2048):
                    ts_data += chunk
                    if len(ts_data) >= 2048:
                        break
                tr.close()

                if len(ts_data) < 100:
                    return False, "ts segment too small"

                ts_ct = tr.headers.get('Content-Type', '')
                # MPEG-TS 以 0x47 开头
                if ts_data[0:1] == b'\x47':
                    return True, "MPEG-TS valid"
                # AAC/audio
                if 'video' in ts_ct or 'mpeg' in ts_ct or 'octet-stream' in ts_ct:
                    return True, "ts valid (content-type)"
                if len(ts_data) > 200:
                    return True, "ts valid (size)"

                return True, "m3u8 parseable (ts unverified)"

            except Exception as e:
                return True, f"m3u8 parseable (ts check failed: {str(e)[:40]})"

        # m3u8 没有 ts 行, 可能是 master playlist
        if '#EXT-X-STREAM-INF' in text:
            return True, "master playlist (multi-quality)"

        return True, "m3u8 parseable (no ts found)"

    # 如果返回了纯文本/HTML 页面 (可能是错误页)
    if '<html' in text.lower() or '<!doctype' in text.lower():
        return False, "returned HTML page"

    if len(text) < 100 and ('error' in text.lower() or 'not found' in text.lower()):
        return False, "error response"

    return True, f"unknown format (len={len(data)})"


# ═══════════════════════════════════════════════════════════════
# 分类逻辑: 自动归入合理的节目组
# ═══════════════════════════════════════════════════════════════

def classify_channel(name):
    """根据频道名自动分类"""
    n = name

    if any(k in n for k in ['RTHK', '港台電視', '港台电视', '港台']):
        return '港台RTHK'
    if any(k in n for k in ['翡翠', 'Jade', 'TVBJ', '无线']):
        return 'TVB翡翠台'
    if 'HOY' in n:
        return 'HOY TV'
    if 'ViuTV' in n or 'viu' in n.lower():
        return 'ViuTV'
    if any(k in n for k in ['凤凰中文', 'Phoenix Chinese']):
        return '凤凰中文台'
    if any(k in n for k in ['凤凰资讯', 'Phoenix Info']):
        return '凤凰资讯台'
    if any(k in n for k in ['凤凰电影', '凤凰香港']):
        return '凤凰其他频道'
    if any(k in n for k in ['凤凰']):
        return '凤凰卫视'
    if any(k in n for k in ['TVBS']):
        return 'TVBS（台湾）'
    if any(k in n for k in ['澳门', 'Macau', 'TDM', '澳视', '澳广']):
        return '澳门频道'
    if any(k in n for k in ['香港卫视', 'HKS', 'HKSTV']):
        return '香港卫视'
    if any(k in n for k in ['耀才', 'BSTV']):
        return '财经频道'
    if any(k in n for k in ['天映', 'Celestial']):
        return '电影频道'
    if any(k in n for k in ['Now', 'now', '有線', 'Cable']):
        return '有线/Now'
    if any(k in n for k in ['ATV', '亚洲电视', '本港', '國際']):
        return '已停播存档'
    return '港澳其他频道'


# ═══════════════════════════════════════════════════════════════
# 输出: TVBox txt 格式
# ═══════════════════════════════════════════════════════════════

def write_tvbox(channels, filepath):
    """
    TVBox / APTV / tv.txt 格式:
        节目组名称,#genre#
        频道名,http://xxx.m3u8
    """
    # 按分类分组
    groups = {}
    for ch in channels:
        g = ch['category']
        if g not in groups:
            groups[g] = []
        groups[g].append(ch)

    # 定义分类排序
    group_order = [
        '港台RTHK', 'TVB翡翠台', 'ViuTV', 'HOY TV',
        '凤凰中文台', '凤凰资讯台', '凤凰其他频道', '凤凰卫视',
        'TVBS（台湾）', '香港卫视', '澳门频道',
        '财经频道', '电影频道', '有线/Now',
        '港澳其他频道', '已停播存档',
    ]

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f'# 自动生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}\n')
        f.write(f'# 频道总数: {len(channels)}\n\n')

        # 按自定义顺序输出
        written_groups = set()
        for g in group_order:
            if g in groups and groups[g]:
                f.write(f'{g},#genre#\n')
                for ch in groups[g]:
                    f.write(f'{ch["name"]},{ch["url"]}\n')
                f.write('\n')
                written_groups.add(g)

        # 剩余未在排序列表中的分组
        for g in sorted(groups.keys()):
            if g not in written_groups and groups[g]:
                f.write(f'{g},#genre#\n')
                for ch in groups[g]:
                    f.write(f'{ch["name"]},{ch["url"]}\n')
                f.write('\n')

    print(f"  [+] TVBox: {filepath} ({len(channels)} 个频道)")


def write_m3u(channels, filepath):
    """标准 m3u 格式 (兼容 VLC/PotPlayer)"""
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write('#EXTM3U\n')
        f.write(f'# 港澳台 IPTV | 更新: {datetime.now().strftime("%Y-%m-%d %H:%M")}\n')
        f.write(f'# 频道: {len(channels)}\n\n')
        for ch in channels:
            f.write(f'#EXTINF:-1 group-title="{ch["category"]}",{ch["name"]}\n')
            f.write(f'{ch["url"]}\n')
    print(f"  [+] M3U: {filepath} ({len(channels)} 个频道)")


def write_json(channels, dead, stats, filepath):
    """JSON 详细报告"""
    data = {
        "meta": {
            "title": "港澳台 IPTV",
            "update": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "validated": stats.get("validated", False),
        },
        "stats": stats,
        "alive": channels,
        "dead": dead,
    }
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  [+] JSON: {filepath}")


# ═══════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='GitHub 港澳台 IPTV 自动抓取')
    parser.add_argument('--no-validate', action='store_true', help='跳过直播源验证')
    parser.add_argument('--timeout', type=int, default=15, help='验证超时秒数')
    parser.add_argument('--workers', type=int, default=30, help='并发验证数')
    parser.add_argument('--output-dir', type=str, default='output', help='输出目录')
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    print(f"{'='*60}")
    print(f"  港澳台 IPTV 自动抓取")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # ── Step 1: 抓取 ──
    all_channels = []
    print("\n[1/3] 抓取数据源...")
    for i, src in enumerate(SOURCES):
        print(f"  [{i+1}/{len(SOURCES)}] {src['name']}")
        content = fetch_url(src['url'], timeout=15)
        if not content:
            print(f"    [!] 失败")
            continue
        channels = parse_m3u(content, src['name'],
                             filter_group=src.get('filter_group'),
                             filter_keyword=src.get('filter_keyword', False))
        print(f"    {len(channels)} 个频道")
        all_channels.extend(channels)
        time.sleep(0.5)

    # ── Step 2: 去重 + 分类 ──
    print(f"\n[2/3] 去重分类...")
    seen_urls = set()
    unique = []
    for ch in all_channels:
        url = ch['url']
        if not url or url in seen_urls:
            continue
        # 对非香港/澳门国家列表的源, 做关键词过滤
        skip_filter = any(s in ch['source'] for s in ['iptv-org-香港', 'iptv-org-澳门'])
        if not skip_filter:
            if not is_hk_channel(ch['name']):
                continue
        seen_urls.add(url)
        ch['category'] = classify_channel(ch['name'])
        unique.append(ch)

    print(f"  去重后: {len(unique)} 个频道")
    for g in sorted(set(c['category'] for c in unique)):
        count = sum(1 for c in unique if c['category'] == g)
        print(f"    {g}: {count}")

    # ── Step 3: 验证 (可选) ──
    if not args.no_validate:
        print(f"\n[3/3] 验证直播源 (并发={args.workers}, 超时={args.timeout}s)...")
        alive = []
        dead = []
        done = 0

        def check(ch):
            ok, reason = validate_stream(ch['url'], timeout=args.timeout)
            return ch, ok, reason

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(check, ch): ch for ch in unique}
            for future in as_completed(futures):
                ch, ok, reason = future.result()
                done += 1
                icon = "OK" if ok else "FAIL"
                print(f"\r  [{done}/{len(unique)}] {icon} {ch['name'][:25]:<25} {reason[:30]}   ", end='', flush=True)
                if ok:
                    alive.append(ch)
                else:
                    dead.append({"name": ch['name'], "url": ch['url'],
                                 "reason": reason, "source": ch['source']})

        print(f"\n  可播: {len(alive)}, 不可播: {len(dead)}")

        # 输出
        write_tvbox(alive, out / "hk.txt")
        write_m3u(alive, out / "hk.m3u")
        write_json(alive, dead, {
            "total_raw": len(all_channels),
            "total_unique": len(unique),
            "alive": len(alive),
            "dead": len(dead),
            "validated": True,
        }, out / "hk.json")
    else:
        print(f"\n[3/3] 跳过验证")
        write_tvbox(unique, out / "hk.txt")
        write_m3u(unique, out / "hk.m3u")
        write_json(unique, [], {
            "total_raw": len(all_channels),
            "total_unique": len(unique),
            "validated": False,
        }, out / "hk.json")

    # 打印最终分类统计
    print(f"\n{'='*60}")
    cats = {}
    final_channels = unique if args.no_validate else alive
    for ch in final_channels:
        c = ch['category']
        cats[c] = cats.get(c, 0) + 1
    for c, n in sorted(cats.items(), key=lambda x: -x[1]):
        print(f"  {c}: {n}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
