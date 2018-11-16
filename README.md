# slow_log_parse

Parses mysql slow logs, aggregates by thread id or query, outputs in json. This is a limited use drop in replacement for the perl version of pt-query-digest

## Getting Started


### Prerequisites

The following go package is used:

```
https://github.com/percona/go-mysql
```

However as I am awaiting on https://github.com/percona/go-mysql/pull/38 to be merged, the following branch is needed:
```
https://github.com/winmutt/go-mysql/tree/thread_id_support
```

## Running the tests

Tests, what are those? percona/go-mysql has great tests covering a wide variety of slow log formats, sadly I do not have any for slow_log_parse yet.

## Contributing

Feel free to submit any PR's, I'll happily work with any contributors.


## Authors

* **Rolf Martin-Hoster** - *Initial work* - [@winmutt](https://github.com/winmutt)

## License

This project is licensed under the GNU Aferro GPL - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* Thanks a bunch to Percona and @arvenil for doing most of the hard work for me
* Thanks to my job for sending me to the Gophercon that kicked off this bit of inspiration
* Thanks to my most excellent coworkers and friends who helped me through this
