import java.time.ZonedDateTime

import main.scala.Utilities
import main.scala.Utilities.UTC
import org.scalatest.FunSuite

class UtilitiesTest extends FunSuite {

  test("testDayOf") {
    assert("2019-07-06" == Utilities.DayOfMs(1562371202000L))
  }

  test("testDayOfTs") {
    assert("2019-07-06" == Utilities.DayOfTs("2019-07-06T00:08:00Z"))
  }

  test("testYearAndMonthOf") {
    assert("2019-07" == Utilities.YearAndMonthOfMs(1562371202000L))
  }

  test("testYearAndMonthOfTs") {
    assert("2019-07" == Utilities.YearAndMonthOfTs("2019-07-06T00:08:00Z"))
  }

  test("testStringYearAndMonthOf") {
    assert("2019-07" == Utilities.YearAndMonthOfString("2019-07-06"))
  }

  test("TimeInterval"){
    assert("2019-07-06T00:00:02Z" == Utilities.TimeInterval(1562371202005L, 1))
    assert("2019-07-06T00:00:10Z" == Utilities.TimeInterval(1562371212005L, 5))
  }

  test("MinuteOf"){
    val second = Utilities.TimeInterval(1562371202000L, 1)
    assert("2019-07-06T00:00:00Z" == Utilities.MinuteOf(1562371202L))
    assert("2019-07-06T00:00:00Z" == Utilities.MinuteOf(1562371200L))
    assert("2019-07-06T00:00:00Z" == Utilities.MinuteOf(1562371259L))
  }

  test("ToUnixTimestampMsDt"){
    assert(1554076890000L == Utilities.CoinapiToEpochMs("2019-04-01T00:01:30.0000000"))
  }

  test("TimeInterval++"){
    val date = ZonedDateTime.of(2019, 7, 6, 0, 12, 22, 10, UTC)
    assert(ZonedDateTime.of(2019, 7, 6, 0, 12, 22, 0, UTC) == Utilities.TimeIntervalFromZonedDateTime(1, date))
    assert(ZonedDateTime.of(2019, 7, 6, 0, 12, 20, 0, UTC) == Utilities.TimeIntervalFromZonedDateTime(5, date))
    assert(ZonedDateTime.of(2019, 7, 6, 0, 12, 0, 0, UTC) == Utilities.TimeIntervalFromZonedDateTime(60, date))
    assert(ZonedDateTime.of(2019, 7, 6, 0, 10, 0, 0, UTC) == Utilities.TimeIntervalFromZonedDateTime(60*5, date))
    assert(ZonedDateTime.of(2019, 7, 6, 0, 10, 0, 0, UTC) == Utilities.TimeIntervalFromZonedDateTime(60*10, date))
    assert(ZonedDateTime.of(2019, 7, 6, 0, 12, 0, 0, UTC) == Utilities.TimeIntervalFromZonedDateTime(60*3, date))
    assert(ZonedDateTime.of(2019, 7, 6, 0, 11, 0, 0, UTC) == Utilities.TimeIntervalFromZonedDateTime(60*11, date))
    assert(ZonedDateTime.of(2019, 7, 6, 0, 12, 15, 0, UTC) == Utilities.TimeIntervalFromZonedDateTime(15, date))
    assert(ZonedDateTime.of(2019, 7, 6, 0, 0, 0, 0, UTC) == Utilities.TimeIntervalFromZonedDateTime(60*60, date))
    assert(ZonedDateTime.of(2019, 7, 6, 1, 0, 0, 0, UTC) == Utilities.TimeIntervalFromZonedDateTime(60*60, date.plusHours(1)))
  }

  test("MinuteOfTheDay"){
    val date = ZonedDateTime.of(2020, 4, 16, 3, 12, 22, 10, UTC)
    val epochSecond = date.toEpochSecond
    assert(3*60 + 12 == Utilities.MinuteOfTheDay(epochSecond, 1))
    assert(3*60 + 10 == Utilities.MinuteOfTheDay(epochSecond, 5))
    assert(3*60 + 10 == Utilities.MinuteOfTheDay(date.minusMinutes(1).toEpochSecond, 5))
    assert(3*60 + 10 == Utilities.MinuteOfTheDay(date.minusMinutes(2).toEpochSecond, 5))
    assert(3*60 + 10 == Utilities.MinuteOfTheDay(date.plusMinutes(1).toEpochSecond, 5))
    assert(3*60 + 10 == Utilities.MinuteOfTheDay(date.plusMinutes(2).toEpochSecond, 5))
    assert(3*60 + 15 == Utilities.MinuteOfTheDay(date.plusMinutes(3).toEpochSecond, 5))
    assert(3*60 == Utilities.MinuteOfTheDay(date.toEpochSecond, 60))
    assert(3*60 == Utilities.MinuteOfTheDay(date.plusMinutes(47).toEpochSecond, 60))
    assert(3*60 + 60 == Utilities.MinuteOfTheDay(date.plusMinutes(48).toEpochSecond, 60))

    assert(3*60 + 12 == Utilities.MinuteOfTheDayMs(date.toInstant.toEpochMilli, 1))
    assert(3*60 + 10 == Utilities.MinuteOfTheDayMs(date.toInstant.toEpochMilli, 5))
  }
}
