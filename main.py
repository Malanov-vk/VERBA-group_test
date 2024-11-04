from data_retrieving import YachtPartsParser


if __name__ == '__main__':
    parser = YachtPartsParser(timeout=3)
    parser.get_all_data()
