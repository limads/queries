with length_avgs as(
    select 
        species,
        avg(sepal_length) as avgs,
        avg(sepal_length) - stddev(sepal_length) as lim_mins,
        avg(sepal_length) + stddev(sepal_length) as lim_maxs,
        avg(sepal_length) + stddev(sepal_length) + 0.2 as lim_maxs_upper
    from iris group by species
) select plot(
    array[
        bars(
            array_agg(avgs),
            '#92b9d8'::text, 
            0.0::real, 
            1.0::real, 
            0.5::real, 
            'f', 
            't'
        ),
        
        intervals(
            array[0.25, 1.25, 2.25]::double precision[],
            array_agg(lim_mins),
            array_agg(lim_maxs),
            '#000000',
            1.0,
            1.0,
            0.1,
            't'
        ),
        
        labels(
            array[0.25, 1.25, 2.25]::double precision[],
            array_agg(lim_maxs_upper),
            array_agg(species),
            '#000000',
            'Liberation Sans 22'
        )
    ],
    
    scale('', 0.0, 3.0, 'tight', 'f', 'f', 0, 0),
    
    scale('Petal length'),
    
    design('#ffffff', '#d3d7cf', 1, 'Monospace Regular 22')
    
) from length_avgs;