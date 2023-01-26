/**
 * Iterates through provided
 * @param {Object} field name of field
 * @param {Object} data Total data set of items
 * @return {Object} list of unique values formatted as options for selection component
 */
export default function prepareSelectOptions(field, data) {
    const optionSet = [];

    /** If data exists, iterate through data set and collect unique values */
    if (data) {
        data.forEach(item => {
            if (optionSet.indexOf(item[field]) === -1) {
                optionSet.push(item[field]);
            }
        })

        /** else no data,  iterate through field object to format values as options */
    } else {
        field.forEach(item => {
            if (optionSet.indexOf(item) === -1) {
                optionSet.push(item);
            }
        });
    }

    optionSet.sort();
    const options = [];

    /** format list as options Object */
    optionSet.forEach((item, index) => options.push({label: item, value: (index + 1).toString()}));

    return options;
}
