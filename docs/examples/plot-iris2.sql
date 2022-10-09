with regression as (
    select generate_series(0.0, 10.0) as x, 
        4.3066 + 0.4089*generate_series(0.0, 10.0) as y
), preds as (
    select line(array_agg(x), array_agg(y)) from regression
), data as (
    select scatter(
        array_agg(petal_length), 
        array_agg(sepal_length),
        '#92b9d8',
        10.0
    ) from iris
) select plot(
    array[
        data.scatter,
        preds.line
    ],
    scale('Petal length'),
    scale('Sepal length')
) from preds cross join data;
