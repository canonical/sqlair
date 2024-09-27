# SQLair
[![Go Reference](https://pkg.go.dev/badge/github.com/canonical/sqlair)](https://pkg.go.dev/github.com/canonical/sqlair)
[![Documentation Status](https://readthedocs.com/projects/canonical-sqlair/badge/?version=latest)](https://canonical-sqlair.readthedocs-hosted.com/en/latest/?badge=latest)
[![Go Report Card](https://goreportcard.com/badge/github.com/canonical/sqlair)](https://goreportcard.com/report/github.com/canonical/sqlair)
[![CI](https://github.com/canonical/sqlair/actions/workflows/go-test.yml/badge.svg)](https://github.com/canonical/sqlair/actions/workflows/go-test.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

SQLair is a Go package and an extension to SQL that makes it easy to map between
Go types and a SQL database.

## Contents

* [:airplane: Fly SQLair](#airplane-fly-sqlair)
* [:dart: Features](#dart-features)
* [:open_book: Documentation](#open_book-documentation)
* [:zap: Get Started](#zap-get-started)
* [:dizzy: Contributing](#dizzy-contributing)

## :airplane: Fly SQLair

Reading and writing to a database in Go can be hard. The solutions out there are
often either too prescriptive (e.g., Object-Relational Mapping (ORM) libraries
where you lose the ability to write your own SQL) or too basic (e.g.,
database/sql, where you have to manually iterate over each row you get back from
the database).

SQLair fills the gap in the middle – automatic type mapping plus the ability to
write any SQL you wish. Write your types directly into the query with _SQLair
expressions_ and SQLair will automatically map them to the query arguments and
inject them with the query results.

If you’re looking to streamline your database game, `go get` SQLair today.

## :dart: Features

 * Maps database inputs and outputs to Go types
 * Simple API
 * Database agnostic
 * Automatic statement caching

## :zap: Get Started

Get started with the [SQLair
tutorial](https://canonical-sqlair.readthedocs-hosted.com/en/latest/tutorial/tutorial/).

## :open_book: Documentation

See the
[Project Documentation](https://canonical-sqlair.readthedocs-hosted.com/en/latest/) and
the [Go Package Reference](https://pkg.go.dev/github.com/canonical/sqlair).

## :dizzy: Contributing

See our [CONTRIBUTING.md](CONTRIBUTING.md).