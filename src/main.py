import uvicorn


def main() -> None:
    uvicorn.run(
        'src.app:get_app',
        host='127.0.0.1',
        port=8001,
        reload=True,
        factory=True,
    )


if __name__ == '__main__':
    main()
