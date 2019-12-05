#!/usr/bin/env python

import click
import numpy as np
import pickle

from sklearn import linear_model, datasets


@click.group()
def main():
    pass


@main.command()
@click.argument("data")
@click.argument("label")
@click.argument("model")
def train(data, label, model):
    X = np.loadtxt(data)
    Y = np.loadtxt(label)

    logreg = linear_model.LogisticRegression(C=1e5)

    logreg.fit(X, Y)

    Y2 = logreg.predict(X)

    num_errors = sum((Y - Y2) * (Y - Y2))
    error = num_errors / Y.shape[0]

    print("Error rate on train is", error)
    pickle.dump(logreg, open(model, 'wb'))


@main.command()
@click.argument("data")
@click.argument("model")
@click.argument("output")
def predict(data, model, output):
    X = np.loadtxt(data)
    logreg = pickle.load(open(model, 'rb'))
    # pred = logreg.decision_function(X)
    pred = logreg.predict_proba(X)[:, 1]
    np.savetxt(output, pred, "%s")


if __name__ == "__main__":
    main()
