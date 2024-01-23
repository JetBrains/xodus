/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore

import java.util.Date
import kotlin.random.Random

class TestData(val kRandom: Random) {

    fun programmingLanguageCreator(): String = programmingLanguageCreators.random(kRandom)
    fun chuckNorrisFact(): String = chuckNorrisFacts.random(kRandom)
    fun rickAndMortyQuote(): String = rickAndMortyQuotes.random(kRandom)

    // Function that returns a random date up in past to specified number of days
    fun pastDateUpToDays(days: Int): Date {
        val randomDays = kRandom.nextInt(days)
        val calendar = java.util.Calendar.getInstance()
        calendar.add(java.util.Calendar.DAY_OF_YEAR, -randomDays)
        return calendar.time
    }

    // return random boolean value
    fun boolean(): Boolean = kRandom.nextBoolean()

    // https://github.com/DiUS/java-faker/blob/master/src/main/resources/en/programming_language.yml
    private val programmingLanguageCreators = arrayOf<String>(
        "John Backus",
        "Friedrich L. Bauer",
        "Gilad Bracha",
        "Walter Bright",
        "Alain Colmerauer",
        "Ole-Johan Dahl",
        "Brendan Eich",
        "James Gosling",
        "Anders Hejlsberg",
        "Rich Hickey",
        "Roberto Ierusalimschy",
        "Alan Kay",
        "Dan Ingalls",
        "Chris Lattner",
        "Yukihiro Matsumoto",
        "John McCarthy",
        "Martin Odersky",
        "Dennis Ritchie",
        "Guido van Rossum",
        "Guy L. Steele, Jr.",
        "Bjarne Stroustrup",
        "Don Syme",
        "Ken Thompson",
        "Larry Wall",
        "Philip Wadler"
    )

    // https://github.com/DiUS/java-faker/blob/master/src/main/resources/en/chuck_norris.yml
    private val chuckNorrisFacts = arrayOf(
        "All arrays Chuck Norris declares are of infinite size, because Chuck Norris knows no bounds.",
        "Chuck Norris doesn't have disk latency because the hard drive knows to hurry the hell up.",
        "All browsers support the hex definitions #chuck and #norris for the colors black and blue.",
        "Chuck Norris can't test for equality because he has no equal.",
        "Chuck Norris doesn't need garbage collection because he doesn't call .Dispose(), he calls .DropKick().",
        "Chuck Norris's first program was kill -9.", "Chuck Norris burst the dot com bubble.",
        "Chuck Norris writes code that optimizes itself.",
        "Chuck Norris can write infinite recursion functions... and have them return.",
        "Chuck Norris can solve the Towers of Hanoi in one move.",
        "The only pattern Chuck Norris knows is God Object.",
        "Chuck Norris finished World of Warcraft.",
        "jetbrains.exodus.entitystore.Project managers never ask Chuck Norris for estimations... ever.",
        "Chuck Norris doesn't use web standards as the web will conform to him.",
        "\"It works on my machine\" always holds true for Chuck Norris.",
        "Whiteboards are white because Chuck Norris scared them that way.",
        "Chuck Norris's beard can type 140 wpm.",
        "Chuck Norris can unit test an entire application with a single assert.",
        "Chuck Norris doesn't bug hunt, as that signifies a probability of failure. He goes bug killing.",
        "Chuck Norris's keyboard doesn't have a Ctrl key because nothing controls Chuck Norris.",
        "Chuck Norris doesn't need a debugger, he just stares down the bug until the code confesses.",
        "Chuck Norris can access private methods.",
        "Chuck Norris can instantiate an abstract class.",
        "Chuck Norris doesn't need to know about class factory pattern. He can instantiate interfaces.",
        "The class object inherits from Chuck Norris.",
        "For Chuck Norris, NP-Hard = O(1).",
        "Chuck Norris knows the last digit of PI.",
        "Chuck Norris can divide by zero.",
        "Chuck Norris doesn't get compiler errors, the language changes itself to accommodate Chuck Norris.",
        "The programs that Chuck Norris writes don't have version numbers because he only writes them once. If a user reports a bug or has a feature request they don't live to see the sun set.",
        "Chuck Norris doesn't believe in floating point numbers because they can't be typed on his binary keyboard.",
        "Chuck Norris solved the Travelling Salesman problem in O(1) time.",
        "Chuck Norris never gets a syntax error. Instead, the language gets a DoesNotConformToChuck error.",
        "No statement can catch the ChuckNorrisException.",
        "Chuck Norris doesn't program with a keyboard. He stares the computer down until it does what he wants.",
        "Chuck Norris doesn't pair program.",
        "Chuck Norris can write multi-threaded applications with a single thread.",
        "There is no Esc key on Chuck Norris' keyboard, because no one escapes Chuck Norris.",
        "Chuck Norris doesn't delete files, he blows them away.",
        "Chuck Norris can binary search unsorted data.",
        "Chuck Norris breaks RSA 128-bit encrypted codes in milliseconds.",
        "Chuck Norris went out of an infinite loop.",
        "Chuck Norris can read all encrypted data, because nothing can hide from Chuck Norris.",
        "Chuck Norris hosting is 101% uptime guaranteed.",
        "When a bug sees Chuck Norris, it flees screaming in terror, and then immediately self-destructs to avoid being roundhouse-kicked.",
        "Chuck Norris rewrote the Google search engine from scratch.",
        "Chuck Norris doesn't need the cloud to scale his applications, he uses his laptop.",
        "Chuck Norris can access the DB from the UI.",
        "Chuck Norris' protocol design method has no status, requests or responses, only commands.",
        "Chuck Norris' programs occupy 150% of CPU, even when they are not executing.",
        "Chuck Norris can spawn threads that complete before they are started.",
        "Chuck Norris programs do not accept input.",
        "Chuck Norris doesn't need an OS.",
        "Chuck Norris can compile syntax errors.",
        "Chuck Norris compresses his files by doing a flying round house kick to the hard drive.",
        "Chuck Norris doesn't use a computer because a computer does everything slower than Chuck Norris.",
        "You don't disable the Chuck Norris plug-in, it disables you.",
        "Chuck Norris doesn't need a java compiler, he goes straight to .war",
        "Chuck Norris can use GOTO as much as he wants to. Telling him otherwise is considered harmful.",
        "There is nothing regular about Chuck Norris' expressions.",
        "Quantum cryptography does not work on Chuck Norris. When something is being observed by Chuck it stays in the same state until he's finished.",
        "There is no need to try catching Chuck Norris' exceptions for recovery; every single throw he does is fatal.",
        "Chuck Norris' beard is immutable.",
        "Chuck Norris' preferred IDE is hexedit.",
        "Chuck Norris is immutable. If something's going to change, it's going to have to be the rest of the universe.",
        "Chuck Norris' addition operator doesn't commute; it teleports to where he needs it to be.",
        "Anonymous methods and anonymous types are really all called Chuck Norris. They just don't like to boast.",
        "Chuck Norris doesn't have performance bottlenecks. He just makes the universe wait its turn.",
        "Chuck Norris does not use exceptions when programming. He has not been able to identify any of his code that is not exceptional.",
        "When Chuck Norris' code fails to compile the compiler apologises.",
        "Chuck Norris does not use revision control software. None of his code has ever needed revision.",
        "Chuck Norris can recite π. Backwards.",
        "When Chuck Norris points to null, null quakes in fear.",
        "Chuck Norris has root access to your system.",
        "When Chuck Norris gives a method an argument, the method loses.",
        "Chuck Norris' keyboard doesn't have a F1 key, the computer asks him for help.",
        "When Chuck Norris presses Ctrl+Alt+Delete, worldwide computer restart is initiated."
    )

    // https://github.com/DiUS/java-faker/blob/master/src/main/resources/en/rick_and_morty.yml
    private val rickAndMortyQuotes = arrayOf(
        "Ohh yea, you gotta get schwifty.",
        "I like what you got.",
        "Don’t even trip dawg.",
        "Get off the high road Summer. We all got pink eye because you wouldn't stop texting on the toilet.",
        "Yo! What up my glip glops!",
        "It's fine, everything is fine. Theres an infinite number of realities Morty and in a few dozen of those I got lucky and turned everything back to normal.",
        "Sometimes science is a lot more art, than science. A lot of people don't get that.",
        "There is no god, Summer; gotta rip that band-aid off now you'll thank me later.",
        "WUBBA LUBBA DUB DUBS!!!",
        "Oh, I'm sorry Morty, are you the scientist or are you the kid who wanted to get laid?",
        "This isn't Game of Thrones, Morty.",
        "You're our boy dog, don't even trip.",
        "He's not a hot girl. He can't just bail on his life and set up shop in someone else's.",
        "I don't get it and I don't need to.",
        "Pluto's a planet.",
        "HI! I'M MR MEESEEKS! LOOK AT ME!",
        "Existence is pain to a meeseeks Jerry, and we will do anything to alleviate that pain.",
        "Well then get your shit together. Get it all together and put it in a backpack, all your shit, so it's together. ...and if you gotta take it somewhere, take it somewhere ya know? Take it to the shit store and sell it, or put it in a shit museum. I don't care what you do, you just gotta get it together... Get your shit together.",
        "Aw, c'mon Rick. That doesn't seem so bad.",
        "Aww, gee, you got me there Rick.",
        "You're like Hitler, except...Hitler cared about Germany, or something.",
        "Hello Jerry, come to rub my face in urine again?",
        "Snuffles was my slave name, you can call me snowball because my fur is pretty and white.",
        "Go home and drink, grandpa.",
        "I'm the devil. What should I do when I fail? Give myself an ice cream?",
        "Weddings are basically funerals with cake.",
        "What about the reality where Hitler cured cancer, Morty? The answer is: Don't think about it.",
        "Nobody exists on purpose. Nobody belongs anywhere. Everybody is going to die.",
        "That just sounds like slavery with extra steps.",
        "Keep Summer safe.",
        "Where are my testicles, Summer?",
        "Oh yeah, If you think my Rick is Dead, then he is alive. If you think you're safe, then he's coming for you.",
        "Let me out, what you see is not the same person as me. My life's a lie. I'm not who you're looking. Let me out. Set me free. I'm really old. This isn't me. My real body is slowly dieing in a vat. Is anybody listening? Can anyone understand? Stop looking at me like that and actually help me. Help me. Help me I'm gunna die.",
        "This sounds like something The One True Morty might say.",
        "I'm more than just a hammer.",
        "That's the difference between you and me, Morty. I never go back to the carpet store.",
        "What is my purpose. You pass butter. Oh My God. Yeah, Welcome to the club pal.",
        "Meeseeks are not born into this world fumbling for meaning, Jerry! We are created to serve a single purpose, for which we go to any lengths to fulfill.",
        "It's a figure of speech, Morty! They're bureaucrats! I don't respect them. Just keep shooting, Morty! You have no idea what prison is like here!",
        "Having a family doesn't mean that you stop being an individual.",
        "Traditionally, science fairs are a father/son thing. Well, scientifically, traditions are an idiot thing.",
        "No no, If I wanted to be sober, I wouldn’t have gotten drunk.",
        "I hate to break it to you, but what people call 'love' is just a chemical reaction that compels animals to breed. It hits hard Morty then it slowly fades leaving you stranded in a failing marriage. I did it. Your parents are going to do it. Break the cycle Morty, rise above, focus on science.",
        "I want that Mulan McNugget sauce, Morty!",
        "Listen, I'm not the nicest guy in the universe, because I'm the smartest, and being nice is something stupid people do to hedge their bets.",
        "Can somebody just let me out of here? If I die in a cage I lose a bet.",
        "Uncertainty is inherently unsustainable. Eventually, everything either is or isn't.",
        "The first rule of space travel kids is always check out distress beacons. Nine out of ten times it's a ship full of dead aliens and a bunch of free shit! One out of ten times it's a deadly trap, but... I'm ready to roll those dice!",
        "Great, now I have to take over an entire planet because of your stupid boobs.",
        "Oh Summer, haha first race war, huh?",
        "Little tip, Morty. Never clean DNA vials with your spit.",
        "So what if the most meaningful day in your life was a simulation operating at minimum complexity."
    )
}