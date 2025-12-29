"""
Google広告 SQLエージェント - エントリーポイント
自然言語でGoogle広告データベースを検索できます
"""

from src.agents.sql_agent import ask_with_details


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

            result = ask_with_details(question)
            print(f"\n【チェック済みSQL】\n{result['checked_query']}")
            print(f"\n【実行結果】\n{result['sql_result']}")
            print(f"\n【回答】\n{result['answer']}")
            if result.get("error"):
                print(f"\n【エラー】\n{result['error']}")

        except KeyboardInterrupt:
            print("\n\n終了します。")
            break
        except Exception as e:
            print(f"\nエラーが発生しました: {e}")


if __name__ == "__main__":
    main()
