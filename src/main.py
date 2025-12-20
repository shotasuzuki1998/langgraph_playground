"""
Google広告 SQLエージェント - エントリーポイント
自然言語でGoogle広告データベースを検索できます
"""

import sys

from src.agents.sql_agent import ask, ask_with_details


def main():
    """
    メイン関数 - 対話型CLI
    """
    print("=" * 60)
    print("Google広告 SQLエージェント")
    print("自然言語でデータベースを検索できます")
    print("終了するには 'exit' または 'quit' を入力")
    print("=" * 60)

    while True:
        try:
            question = input("\n質問: ").strip()

            if not question:
                continue

            if question.lower() in ("exit", "quit", "q"):
                print("終了します。")
                break

            # 詳細モード（--detailオプション）
            show_detail = False
            if question.startswith("--detail "):
                show_detail = True
                question = question[9:].strip()

            print("\n処理中...")

            if show_detail:
                result = ask_with_details(question)
                print(f"\n【チェック済みSQL】\n{result['checked_query']}")
                print(f"\n【実行結果】\n{result['sql_result']}")
                print(f"\n【回答】\n{result['answer']}")
                if result.get("error"):
                    print(f"\n【エラー】\n{result['error']}")
            else:
                answer = ask(question)
                print(f"\n{answer}")

        except KeyboardInterrupt:
            print("\n\n終了します。")
            break
        except Exception as e:
            print(f"\nエラーが発生しました: {e}")


def run_demo():
    """
    デモ実行 - サンプル質問を実行
    """
    questions = [
        "2024年の総広告費用を教えて",
        "キャンペーン別のコンバージョン数を集計して",
        "CTRが最も高いキーワードTOP5は？",
    ]

    for q in questions:
        print(f"\n{'=' * 60}")
        print(f"質問: {q}")
        print("=" * 60)

        result = ask_with_details(q)

        print(f"\n【生成されたSQL】\n{result['sql_query']}")
        print(f"\n【チェック済みSQL】\n{result['checked_query']}")
        print(f"\n【実行結果】\n{result['sql_result']}")
        print(f"\n【回答】\n{result['answer']}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--demo":
        run_demo()
    else:
        main()
