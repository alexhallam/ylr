<h1 align="center">Yeller (ylr)</h1>
<p align="center">As in Ol' Yeller</p>
<p align="center">Data's Best Friend</p>

<p align="center">
  <img src="https://storage.googleapis.com/data_xvzf/ylr_small_docs_logo.png" alt="Logo" width="100" height="100">
</p>

# What is This

I dplyr-like language of data manipulation, which uses arrow as its backend and parquet as its storage format. It is designed to be a lightweight, fast, and fun.

# Grammar of Data Manipulation In A DBMS

While I respect and enjoy the SQL-ISO standard. It is hard to beat the grammar of `dplyr` and other tidyverse packages. For this reason you will notice `ylr` replicates functions from `dplyr` as close as possible.

It is hard to call this a proper database. Many databsaes have their own files. For example, `duckdb` (one of the greats) has its own file format (`*.db`). One disadvantage of this is that it is hard to decouple the database from the data.

`ylr` does not share this concept. A "database" is a folder of parquet files. A "table" is a parquet file. So if you decide to leave `ylr` and use other programs to manipulate your data you are free to do so with no overhead. Also, sharing a database amounts to zipping the folder and sending it to someone else.


# Reading data

```r
# print on read so you always know what you are working with

# read_csv files
rstartwars <- read_csv("https://raw.githubusercontent.com/fivethirtyeight/data/master/star-wars-survey/StarWars.csv")

# read_parquet files
read_parquet("some.parquet")

# if data is already in the ylr database then a table name can be used
# <directory_name>.<file_name>
main.starwars
```

# Creating Data

```r
a = tribble(~x, ~y, ~z, 
            "a", 2, 3.6, 
            "b", 1, 8.5)
# print a
a
```

# Writing data

```r
a = tribble(~x, ~y, ~z, 
            "a", 2, 3.6, 
            "b", 1, 8.5)

# write to database
a |>
    write_table("dir", "table_name")

# write to a csv somewhere
a |>
    write_csv("some_dir/some.csv") 

# write to a parquet file somewhere
a |>
    write_csv("some_dir/some.pq")
```

# Is `ylr` a programming language?

No, it is a domain specific language. It is designed to be a lightweight, fast, and fun. It is not designed to be a general purpose programming language. If you want to do something that `ylr` does not support then you should use a general purpose programming language. 

It is best to think of `yrl` as a tool to manipulate data. It is not a tool to do data science. It is not a tool to do machine learning. It is not a tool to do anything other than manipulate data in the most fun way possible.

# Data Types

SQLite has been able to get away with only 5 data types. This is mostly a good thing. Some complain about not having a `date` data type, but they are able to find work arounds with strings just fine. 

It is hard to beat the simplicity of SQLite. More recent databases and data manipulation libraries have a dozen or more datatypes.

In `ylr` columns can have one of the following data types (`Null` are permitted as values but not as column types.):

| Data Type  | Abbreviation | Example               | Details                                                                     |
| ---------- | ------------ | --------------------- | --------------------------------------------------------------------------- |
| `bool`     | `bool`       | `true`,`false`        | Rust `bool`                                                                 |
| `i64`      | `int`        | `1,2,3`               | Rust `i64`                                                                  |
| `f64`      | `dbl`        | `1.4, 1.3`            | Rust `f64`                                                                  |
| `String`   | `char`       | `"hi"`                | Users can enter strings without `*.to_string()` when using `vec_c!()` macro |
| `Date`     | `date`       | `2023-01-01`          | From `arrow::DataTypes` or `Chrono`                                         |
| `DateTime` | `ts`         | `2023-01-01 23:40:00` | From `arrow::DataTypes` or `Chrono`                                         |

# Coersion Rules

The `ylr` database follows the president set by 

| Type   | `bool` | `int` | `dbl` | `char` | `date` | `ts` |
| ------ | ------ | ----- | ----- | ------ | ------ | ---- |
| `bool` | V      | V     | V     |        |        |      |
| `int`  | V      | V     | V     |        |        |      |
| `dbl`  | V      | V     | V     |        |        |      |
| `char` |        |       |       | V      |        |      |
| `date` |        |       |       |        | V      | V    |
| `ts`   |        |       |       |        | V      | V    |

# References

Nearly none of the ideas in this package are original. I have borrowed heavily from the following packages, authors, languages:

# vctrs
```
  Wickham H, Henry L, Vaughan D (2022). _vctrs: Vector
  Helpers_. R package version 0.5.1,
  <https://CRAN.R-project.org/package=vctrs>.

A BibTeX entry for LaTeX users is

  @Manual{,
    title = {vctrs: Vector Helpers},
    author = {Hadley Wickham and Lionel Henry and Davis Vaughan},
    year = {2022},
    note = {R package version 0.5.1},
    url = {https://CRAN.R-project.org/package=vctrs},
  }

```

# GNU-R

```
  R Core Team (2022). R: A language and environment for
  statistical computing. R Foundation for Statistical
  Computing, Vienna, Austria. URL https://www.R-project.org/.

A BibTeX entry for LaTeX users is

  @Manual{,
    title = {R: A Language and Environment for Statistical Computing},
    author = {{R Core Team}},
    organization = {R Foundation for Statistical Computing},
    address = {Vienna, Austria},
    year = {2022},
    url = {https://www.R-project.org/},
  }

We have invested a lot of time and effort in creating R, please
cite it when using it for data analysis. See also
‘citation("pkgname")’ for citing R packages.
```

# Pillar

```
  Müller K, Wickham H (2022). _pillar: Coloured Formatting for
  Columns_. R package version 1.8.1,
  <https://CRAN.R-project.org/package=pillar>.

A BibTeX entry for LaTeX users is

  @Manual{,
    title = {pillar: Coloured Formatting for Columns},
    author = {Kirill Müller and Hadley Wickham},
    year = {2022},
    note = {R package version 1.8.1},
    url = {https://CRAN.R-project.org/package=pillar},
  }

```

# Tibble

```
  Müller K, Wickham H (2022). _tibble: Simple Data Frames_. R
  package version 3.1.8,
  <https://CRAN.R-project.org/package=tibble>.

A BibTeX entry for LaTeX users is

  @Manual{,
    title = {tibble: Simple Data Frames},
    author = {Kirill Müller and Hadley Wickham},
    year = {2022},
    note = {R package version 3.1.8},
    url = {https://CRAN.R-project.org/package=tibble},
  }
```

# dplyr

```
  Wickham H, François R, Henry L, Müller K (2022). _dplyr: A
  Grammar of Data Manipulation_. R package version 1.0.10,
  <https://CRAN.R-project.org/package=dplyr>.

A BibTeX entry for LaTeX users is

  @Manual{,
    title = {dplyr: A Grammar of Data Manipulation},
    author = {Hadley Wickham and Romain François and Lionel Henry and Kirill Müller},
    year = {2022},
    note = {R package version 1.0.10},
    url = {https://CRAN.R-project.org/package=dplyr},
  }

```

# Details On Implimentation

As a reader I always appreciate some details on how things work. As an author I will try my best to be as transparent as possible as it relates to the implimentation of `ylr`.

```rust
// vec_c! initially takes all of the main datatypes and converts them to a `ValueRaw` enum.
// If I were to "pause" the program here, I would have a vector of `ValueRaw` enums.
// After the enum is created, the coercion rules are applied to the vector
fn main() {
    let mixed_vex: Vec<ValueRaw> = vec_c!(0.5,42,false,"hi","2021-03-25","2021-03-25 23:30:10",Null);
    dbg!(mixed_vex);
}
//     Float64(
//         0.5,
//     ),
//     Int64(
//         42,
//     ),
//     Boolean(
//         false,
//     ),
//     Utf8(
//         "hi",
//     ),
//     Date(
//         2021-03-25,
//     ),
//     DateTime(
//         2021-03-25T23:30:10,
//     ),
//     Null(
//         Null,
//     ),
// ]
```

At this point a table is made of the pairwise types given in `vec_c!()`

```
vec_c!(0.5,42,false,"hi","2021-03-25","2021-03-25 23:30:10",Null)    
```

| first   | second  | types           | coercion_pair |
| ------- | ------- | --------------- | ------------- |
| `0.5`   | `42`    | `dbl` & `int`   | `dbl`         |
| `42`    | `false` | `int`  & `bool` | `int`         |
| `false` | `"hi"`  | `bool` & `chr`  | `Error`       |

As soon as an error is hit the program stops and returns an error. If no errors are hit, the program continues and the table is used to coerce the `ValueRaw` enum to the correct type. The coercion type is the maximum type of all the coercion pairs.


1. `bool > int > dbl`

2. `char` is the only type that is not coerced to a higher type. It is its own thing.

3. `date > ts`


# Differences between `ylr` and `dplyr`

I have made every effort possible to make the syntax in `ylr` index on `dplyr`. The `ylr` package is a domain specific language not a language on its own. The differences which were unavoidable are due to the fact that Rust is not R. For this reason there are some unavoidable differences between `ylr` and `dplyr`. These differences are minor, but could cause some slight iteration. If symptoms continue, please see your doctor.

| `dplyr` | `ylr`  |
| ------- | ------ |
| `NA`    | `Null` |


# What is ylr ... again?

There are examples of mixing good things and something good coming out and mixing good things and bad coming out. Time will tell how the chemistry works out.

| good mix                    | bad mix                             |
| --------------------------- | ----------------------------------- |
| Peanut butter and chocolate | Fruit and cake (Jim Gaffigan et al) |

That said here are some things I like from some of my favorite technologies.

```
                       +------+
               duckdb. | OLAP |
             SQLite .. +------+-+
                    .. |Embedded|
                    .. +---+----+
                    .. |CLI|
                    .. +---+----------+
                    .. |Single File DB|
GNU-R And Tidyverse... +--------------+-+
                   ..  |Handful of types|
                   .   +-----+----------+
                   .   |Pipes|
                   .   +-----+---------+
                   .   |Tidyverse Verbs|
                   .   +-------------+-+
      Apache Arrow.    |Feather Files|
                  .    +------------++
                  .    |Arrow Memory|
                  .    +------------+--+
           Polars.     | Chunked Arrays|
                 .     +---------------+-------+
                 .     |Rust For Data Wrangling|
                       +-----------------------+
```