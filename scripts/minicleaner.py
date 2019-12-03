import click
import lzma
import fasttext
import sys


@click.command()
@click.option("--inp", help="input")
@click.option("--lid", help="Language identification implementation")
@click.option("--lid_model", help="fastText LID model path")
@click.option("--lang1", required=True, help="")
@click.option("--lang2", required=True, help="")
@click.option("--err_out")
def main(inp, lid, lid_model, lang1, lang2, err_out):
    try:
        if inp:
            if inp[-2:] == "xz":
                reader = lzma.open(inp)
            else:
                reader = open(inp)
        else:
            reader = sys.stdin

        if err_out:
            err = lzma.open(err_out, "wt")
        else:
            err = sys.stderr

        if lid == "fastText":
            model = fasttext.load_model(lid_model)
        else:
            raise ValueError("LID out of bounds")

        for line in reader:
            if isinstance(line, bytes):
                line = line.decode("utf8")
            line = line.strip()
            s1, s2 = line.split("\t")[2:4]
            if s1.strip() == s2.strip():
                print(f"eq: {line}", file=err)
                continue

            pred1 = model.predict([s1])[0]
            pred2 = model.predict([s2])[0]
            if pred1[0][0][9:] == lang1 and pred2[0][0][9:] == lang2:
                print(line, file=sys.stdout)
            else:
                print(f"lid: {line}", file=err)

    finally:
        if inp and reader:
            reader.close()
        if err_out and err:
            err.close()


if __name__ == "__main__":
    main()
