"""
Google広告 SQLエージェント - エントリーポイント
"""

from src.agents.sql_agent import ask_with_details


def main():
    print("=" * 60)
    print("Google広告 SQLエージェント")
    print("自然言語でデータベースを検索できます")
    print("終了するには 'exit' または 'quit' を入力")
    print("=" * 60)

    show_debug = False

    while True:
        try:
            question = input("\n質問: ").strip()

            if not question:
                continue

            if question.lower() in ("exit", "quit", "q"):
                print("終了します。")
                break

            if question.lower() == "debug":
                show_debug = not show_debug
                print(f"→ デバッグ表示: {'ON' if show_debug else 'OFF'}")
                continue

            result = ask_with_details(question)

            print(f"\n【チェック済みSQL】\n{result['checked_query']}")

            if show_debug and result.get("evidence_graph_prompt"):
                print(f"\n【Evidence Graph】\n{result['evidence_graph_prompt']}")

            print(f"\n【回答】\n{result['answer']}")

            if result.get("error"):
                print(f"\n【エラー】\n{result['error']}")

        except KeyboardInterrupt:
            print("\n\n終了します。")
            break
        except Exception as e:
            print(f"\nエラーが発生しました: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    main()
