var stream = require('stream');

exports.retrieve = function(sql, params, retrieveFn, rowMappingFn, filterFn) {
    return new Promise((resolve, reject) => {
        retrieveFn(sql, params, (err, result) => {
            if (err) {
                reject(err);
            } else {
                if (rowMappingFn) {
                    rowMappingFn(result, (postErr, finalResult) => {
                        if (postErr) {
                            reject(postErr);
                        } else {
                            if (filterFn) {
                                finalResult.rows = finalResult.rows.filter(filterFn);
                            }
                            resolve(finalResult);
                        }
                    });
                } else {
                    resolve(result);
                }
            }
        });
    });
}


exports.retrieveStream = function(sql, params, streamFn, rowMappingFn) {
    if (postProcessRowFn) {
        var transformStream = new stream.Transform();
        transformStream._transform = (data, encoding, callback) => {
            rowMappingFn(data, callback);
        };
        streamFn(sql, params)
            .pipe(transformStream);
        return transformStream;
    } else {
        return streamFn(sql, params);
    }

}

exports.update = function(sql, params, updateFn, resultMappingFn) {
    return new Promise((resolve, reject) => {
        updateFn(sql, params, (err, result) => {
            if (err) {
                reject(err);
            } else {
                if (resultMappingFn) {
                    resultMappingFn(result, (mapErr, finalResult) => {
                        if (mapErr) {
                            reject(mapErr);
                        } else {
                            resolve(finalResult);
                        }
                    })
                } else {
                    resolve(result);
                }
            }
        });
    });
}

