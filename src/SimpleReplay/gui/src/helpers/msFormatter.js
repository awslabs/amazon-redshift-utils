/**
 * Converts ms value to min:sec.ms string for visual formatting
 * @param {*} milliseconds, value in milliseconds
 * @param {number} digits, number of digits to round to (default 2)
 * @return {string} Formatted string
 */

export default function millisToMinutesAndSeconds(milliseconds, digits = 2) {
  const minutes = Math.floor(milliseconds / 60000);
  const seconds = ((milliseconds % 60000) / 1000).toFixed(digits);

  return minutes + ":" + (seconds < 10 ? '0' : '') + seconds;
}