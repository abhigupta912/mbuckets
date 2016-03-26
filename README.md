# mbuckets
[![Build Status](https://drone.io/github.com/abhigupta912/mbuckets/status.png)](https://drone.io/github.com/abhigupta912/mbuckets/latest) [![GoDoc](https://godoc.org/github.com/abhigupta912/mbuckets?status.svg)](https://godoc.org/github.com/abhigupta912/mbuckets) [![Build Status](https://travis-ci.org/abhigupta912/mbuckets.svg?branch=master)](https://travis-ci.org/abhigupta912/mbuckets) [![Coverage Status](https://coveralls.io/repos/github/abhigupta912/mbuckets/badge.svg?branch=master)](https://coveralls.io/github/abhigupta912/mbuckets?branch=master)

A simple wrapper over [BoltDB](https://github.com/boltdb/bolt) that allows easy operations on multi level (nested) buckets.

## Overview

mbuckets is heavily inspired by [Buckets](https://github.com/joyrexus/buckets).

mbuckets originated from a need to store data under heirarchial paths.

## Installation

```bash
go get -u github.com/abhigupta912/mbuckets
```

## Usage

See [mbuckets_test.go](https://github.com/abhigupta912/mbuckets/blob/master/mbuckets_test.go) for examples.

