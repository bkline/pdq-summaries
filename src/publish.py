#!/usr/bin/env python3

"""
Push summary documents to a Drupal CMS server.
"""

from argparse import ArgumentParser
from copy import deepcopy
from datetime import datetime
from functools import cached_property
from json import dumps as json_dumps, loads as json_loads
from logging import basicConfig, getLogger
from os import chdir, getenv
from pathlib import Path
from re import compile as re_compile, sub as re_sub
from time import sleep
from urllib.parse import urlparse
from lxml import etree, html
from lxml.html import builder as B
from requests import get, patch, post


class Control:
    """Top-level object for CDR publishing job processing"""

    FMT = "%(asctime)s [%(levelname)s] %(message)s"
    LOG = "publish.log"
    BASE = "http://www.devbox"

    def run(self):
        """Top-level entry point for the script"""

        start = datetime.now()
        pushed = []
        for doc in self.docs:
            if self.dump_dir:
                doc.dump()
            else:
                nid = self.client.push(doc.values)
                pushed.append((doc.id, nid, doc.langcode))
        if not self.dump_dir:
            errors = self.client.publish(pushed)
            if errors:
                msg = f"{len(errors)} Drupal publish errors; see logs"
                raise Exception(msg)
        else:
            print(f"dumped {len(self.docs)} summaries to {self.dump_dir}")
        elapsed = datetime.now() - start
        verb = "Dumped" if self.dump_dir else "Sent"
        self.logger.info("%s %d docs in %s", verb, len(self.docs), elapsed)

    @cached_property
    def auth(self):
        """Credentials for the CMS"""
        password = self.get_secret("PDQ_PASSWORD")
        if not password:
            raise Exception("credentials for PDQ account are required")
        return "PDQ", password

    @cached_property
    def catalog(self):
        """Dictionary of all available summaries, indexed by CDR ID"""

        catalog = {}
        root = Path("../docs")
        for type in ("cis", "dis"):
            cls = CIS if type == "cis" else DIS
            for langcode in ("en", "es"):
                for path in root.glob(f"{type}/{langcode}/*.xml"):
                    id = int(path.stem)
                    catalog[id] = cls(self, path)
        return catalog

    @cached_property
    def client(self):
        """What we use to talk to the Drupal CMS"""
        return DrupalClient(self)

    @cached_property
    def docs(self):
        """Sequence of CIS and/or DIS objects"""

        if self.opts.ids:
            docs = []
            for doc_id in self.opts.ids:
                if doc_id not in self.catalog:
                    raise Exception(f"CDR{doc_id} not found")
                docs.append(self.catalog[doc_id])
            return sorted(docs)
        docs = self.catalog.values()
        if self.opts.type:
            docs = [d for d in docs if d.TYPE == self.opts.type]
        docs = sorted(docs)
        if self.opts.max is not None or self.opts.skip is not None:
            start = self.opts.skip or 0
            if start < 0:
                raise Exception("skip cannot be negative")
            if self.opts.max is None:
                docs = docs[start:]
            elif self.opts.max < 1:
                raise Exception("max cannot be less than 1")
            else:
                end = start + self.opts.max
                docs = docs[start:end]
        if not len(docs):
            self.logger.warning("no summaries to push")
        else:
            self.logger.info("pushing %d summaries", len(docs))
        return sorted(docs)

    @cached_property
    def dump_dir(self):
        """Optional directory in which to store summary JSON"""
        if not self.opts.dump:
            return None
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        path = Path(f"../dumps/{stamp}")
        path.mkdir(parents=True)
        return path

    @cached_property
    def logger(self):
        """Used for recording what we do."""
        logger = getLogger("publish")
        level = "DEBUG" if self.opts.debug else "INFO"
        basicConfig(filename=self.LOG, level=level, format=self.FMT)
        return logger

    @cached_property
    def opts(self):
        """Processing options."""

        types = "cis", "dis"
        base_help = "base URL for CMS (default: http://www.devbox)"
        batch_help = "number to mark as publishable in each call"
        debug_help = "enable debug logging"
        dump_help = "store summary JSON locally instead of pushing it"
        ids_help = "push specific summaries"
        max_help = "maximum number of summaries to push"
        skip_help = "number of summaries to skip past"
        tier_help = "where to link for media on Akamai"
        type_help = "restrict push to single summary type"
        parser = ArgumentParser()
        parser.add_argument("--base", default=self.BASE, help=base_help)
        parser.add_argument("--batch", type=int, help=batch_help)
        parser.add_argument("--debug", action="store_true", help=debug_help)
        parser.add_argument("--dump", action="store_true", help=dump_help)
        parser.add_argument("--ids", type=int, nargs="+", help=ids_help)
        parser.add_argument("--max", type=int, help=max_help)
        parser.add_argument("--skip", type=int, help=skip_help)
        parser.add_argument("--tier", default="PROD", help=tier_help)
        parser.add_argument("--type", choices=types, help=type_help)
        return parser.parse_args()

    @staticmethod
    def get_secret(name, fallback=Path(".secrets.json")):
        """Retrieve a sensitive value

        Try GitHub Actions environment first, then fall back on local file.

        Pass:
            name - string naming the value to retrieve
            fallback - name of file in which to find value

        Return:
            string for the requested value
        """

        value = getenv(name)
        if value:
            return value
        if fallback.exists():
            secrets = json_loads(fallback.read_text())
            return secrets.get(name)
        return None


class Summary:
    """Base class for both type of summaries."""

    DESCRIPTION_MAX = 600
    chdir(Path(__file__).resolve().parent)

    def __init__(self, control, path):
        """Remember the caller and where to find the XML"""
        self.control = control
        self.path = path

    def __lt__(self, other):
        """Sort English before Spanish, then by type and ID"""
        a = self.langcode, self.TYPE, self.id
        b = other.langcode, other.TYPE, other.id
        return a < b

    def dump(self):
        """Save the summary's JSON locally"""
        path = self.control.dump_dir / f"{self.id}.json"
        path.write_text(json_dumps(self.values, indent=2), encoding="utf-8")

    @cached_property
    def id(self):
        """Integer for the summary document's CDR ID"""
        return int(self.path.stem)

    @cached_property
    def langcode(self):
        """Either en or es"""
        return self.path.parts[-2]

    @cached_property
    def logger(self):
        """Used to record what we do"""
        return self.control.logger

    @staticmethod
    def extract_id(arg):
        """
        Return the CDR document ID as an integer (ignoring fragment suffixes)
        """

        if isinstance(arg, bytes):
            arg = arg.decode("utf-8")
        if isinstance(arg, str):
            return int(re_sub(r"[^\d]", "", arg.split("#")[0]))
        return int(arg)

    @staticmethod
    def get_text(node, default=None):
        """
        Assemble the concatenated text nodes for an element of the document.

        Note that the call to node.itertext() must include the wildcard
        string argument to specify that we want to avoid recursing into
        nodes which are not elements. Otherwise we will get the content
        of processing instructions, and how ugly would that be?!?

        Pass:
            node - element node from an XML document parsed by the lxml package
            default - what to return if the node is None

        Return:
            default if node is None; otherwise concatenated string node
            descendants
        """

        if node is None:
            return default
        return "".join(node.itertext("*"))


class CIS(Summary):
    """Cancer Information Summary"""

    TYPE = "cis"
    ABOUT_THIS = "_section_AboutThis_1"
    IN_THIS_SECTION = {
        "en": "In This Section",
        "es": "En esta secci\xf3n",
    }
    BROWSER_TITLE_MAX = 100
    CTHP_CARD_TITLE_MAX = 100
    TRANSFORM = etree.XSLT(etree.parse("cms-cis.xsl"))

    @property
    def values(self):
        """Get the pieces of the summary needed by the Drupal CMS

        Don't cache this property, so we avoid accumulating excessive
        memory usage.
        """

        # Tease out pieces which need a little bit of logic.
        root = etree.parse(self.path).getroot()
        meta = root.find("SummaryMetaData")
        node = meta.find("SummaryURL")
        if node is None:
            raise Exception(f"CDR{self.id:d} has no SummaryURL")
        try:
            url = urlparse(node.get("xref")).path
        except Exception:
            raise Exception(f"CDR{self.id:d}: bad or missing summary URL")
        if not url:
            raise Exception(f"CDR{self.id:d}: missing summary URL")
        if url.startswith("/espanol"):
            url = url[len("/espanol"):]
        browser_title = cthp_card_title = translation_of = None
        for node in root.findall("AltTitle"):
            if node.get("TitleType") == "Browser":
                browser_title = self.get_text(node)
            elif node.get("TitleType") == "CancerTypeHomePage":
                cthp_card_title = self.get_text(node)
        if not cthp_card_title:
            cthp_card_title = browser_title
        node = root.find("TranslationOf")
        if node is not None:
            translation_of = self.extract_id(node.get("ref"))
        svpc = suppress_otp = 0
        if root.get("SVPC") == "Yes":
            svpc = 1
        if root.get("SuppressOnThisPageSection") == "Yes":
            suppress_otp = 1
        partner_merge_set = root.get("PartnerMergeSet") == "Yes"

        # Pull out the summary sections into sequence of separate dictionaries.
        intro_text_index = None
        for i, node in enumerate(root.findall("SummarySection")):
            types = []
            for child in node.findall("SectMetaData/SectionType"):
                types.append(self.get_text(child, ""))
            if "Introductory Text" in types and not partner_merge_set:
                if intro_text_index is not None:
                    error = "CDR{} has multiple introductory text sections"
                    raise Exception(error.format(self.id))
                intro_text_index = i
            else:
                title = self.get_text(node.find("Title"), "").strip()
                if not title and not partner_merge_set and not svpc:
                    if types:
                        types = ", ".join(types)
                        types = f"of type(s) {types}"
                    else:
                        types = "with no section types specified"
                    error = "CDR{} missing title for section {} {}"
                    args = self.id, i + 1, types
                    raise Exception(error.format(*args))
        target = "@@MEDIA-TIER@@"
        tier = self.control.opts.tier.lower()
        replacement = f"-{tier}" if tier != "prod" else ""
        transformed = self.TRANSFORM(root)
        self.__consolidate_citation_references(transformed)
        xpath = 'body/div/article/div[@class="pdq-sections"]'
        sections = []
        intro_text = None
        i = 0
        for node in transformed.xpath(xpath):
            h2 = node.find("h2")
            if h2 is None:
                section_title = ""
            else:
                section_title = self.get_text(h2, "").strip()
                node.remove(h2)
            if intro_text_index != i and node.get("id") != self.ABOUT_THIS:
                if not svpc:
                    headers = list(node.iter("h3", "h4"))
                    if headers and "kpBox" not in headers[0].get("id", ""):
                        links = B.UL()
                        parent = nested_links = None
                        for header in headers:
                            link = deepcopy(header)
                            link.set("href", "#" + link.get("id"))
                            del link.attrib["id"]
                            link.tag = "a"
                            if header.tag == "h3":
                                parent = B.LI(link)
                                nested_links = None
                                links.append(parent)
                            else:
                                if nested_links is None:
                                    nested_links = B.UL(B.LI(link))
                                    if parent is not None:
                                        parent.append(nested_links)
                                    else:
                                        links.append(nested_links)
                                else:
                                    nested_links.append(B.LI(link))
                        h6 = B.H6(self.IN_THIS_SECTION[self.langcode])
                        nav = B.E("nav", h6, links)
                        nav.set("class", "in-this-section")
                        nav.set("role", "navigation")
                        node.insert(0, nav)
            body = html.tostring(node).decode("utf-8")
            body = body.replace(target, replacement)
            if intro_text_index == i:
                intro_text = body
            else:
                section_id = node.get("id")
                if section_id.startswith("_section"):
                    section_id = section_id[len("_section"):]
                sections.append({
                    "title": section_title,
                    "id": section_id,
                    "html": body,
                })
            i += 1

        # Pull everything together.
        audience = self.get_text(meta.find("SummaryAudience"))
        description = self.get_text(meta.find("SummaryDescription"))
        if len(description) > self.DESCRIPTION_MAX:
            self.logger.warning("Truncating description %r", description)
            description = description[:self.DESCRIPTION_MAX]
        if len(browser_title) > self.BROWSER_TITLE_MAX:
            message = "Truncating browser title %r"
            self.logger.warning(message, browser_title)
            browser_title = browser_title[:self.BROWSER_TITLE_MAX]
        if len(cthp_card_title) > self.CTHP_CARD_TITLE_MAX:
            self.logger.warning("Truncating cthp title %r", cthp_card_title)
            cthp_card_title = cthp_card_title[:self.CTHP_CARD_TITLE_MAX]
        return {
            "cdr_id": self.id,
            "url": url,
            "browser_title": browser_title,
            "cthp_card_title": cthp_card_title,
            "translation_of": translation_of,
            "sections": sections,
            "title": self.get_text(root.find("SummaryTitle")),
            "description": description,
            "summary_type": self.get_text(meta.find("SummaryType")),
            "audience": audience.replace(" prof", " Prof"),
            "language": self.langcode,
            "posted_date": self.get_text(root.find("DateFirstPublished")),
            "updated_date": self.get_text(root.find("DateLastModified")),
            "type": "pdq_cancer_information_summary",
            "suppress_otp": suppress_otp,
            "svpc": svpc,
            "intro_text": intro_text,
        }

    def __consolidate_citation_references(self, root):
        """
        Combine adjacent citation reference links

        Ranges of three or more sequential reference numbers should be
        collapsed as FIRST-LAST. A sequence of adjacent refs (ignoring
        interventing whitespace) should be surrounded by a pair of
        square brackets. Both ranges and individual refs should be
        separated by commas. The substring "cit/section" should be
        replaced in the result by "section" (stripping "cit/"). For
        example, with input of ...

          <a href="#cit/section_1.1">1</a>
          <a href="#cit/section_1.2">2</a>
          <a href="#cit/section_1.3">3</a>
          <a href="#cit/section_1.5">5</a>
          <a href="#cit/section_1.6">6</a>

        ... we should end up with ...

          [<a href="section_1.1"
           >1</a>-<a href="section_1.3"
           >3</a>,<a href="section_1.5"
           >5</a>,<a href="section_1.6"
           >6</a>]

        2019-03-13: Bryan P. decided to override Frank's request to
        have "cit/" stripped from the linking URLs.

        Pass:
          root - reference to parsed XML document for the PDQ summary

        Return:
          None (parsed tree is altered as a side effect)
        """

        # Collect all of the citation links, stripping "cit/" from the url.
        # 2019-03-13 (per BP): don't strip "cit/".
        links = []
        for link in root.iter("a"):
            href = link.get("href")
            if href is not None and href.startswith("#cit/section"):
                links.append(link)

        # Collect links which are only separated by optional whitespace.
        adjacent = []
        for link in links:

            # First time through the loop? Start a new list.
            if not adjacent:
                adjacent = [link]
                prev = link

            # Otherwise, find out if this element belongs in the list.
            else:
                if prev.getnext() is link:

                    # Whitespace in between is ignored.
                    if prev.tail is None or not prev.tail.strip():
                        adjacent.append(link)
                        prev = link
                        continue

                # Consolidate the previous list and start a new one.
                self.__rewrite_adjacent_citation_refs(adjacent)
                adjacent = [link]
                prev = link

        # Deal with the final list of adjacent elements, if any.
        if adjacent:
            self.__rewrite_adjacent_citation_refs(adjacent)

    def __rewrite_adjacent_citation_refs(self, links):
        """
        Add punctuation to citation reference links and collapse ranges

        For details, see `consolidate_citation_references()` above.

        Pass:
          nodes - list of adjacent reference link elements

        Return:
          None (the parsed tree is modified in place)
        """

        # Find out where to hang the left square bracket.
        prev = links[0].getprevious()
        parent = links[0].getparent()
        if prev is not None:
            if prev.tail is not None:
                prev.tail += "["
            else:
                prev.tail = "["
        elif parent.text is not None:
            parent.text += "["
        else:
            parent.text = "["

        # Pull out the integers for the reference lines.
        refs = [int(link.text) for link in links]

        # Find ranges of unbroken integer sequences.
        i = 0
        while i < len(refs):

            # Identify the next range.
            range_len = 1
            while i + range_len < len(refs):
                if refs[i+range_len-1] + 1 != refs[i+range_len]:
                    break
                range_len += 1

            # If range is three or more integers, collapse it.
            if range_len > 2:
                if i > 0:
                    links[i-1].tail = ","
                links[i].tail = "-"
                j = 1
                while j < range_len - 1:
                    parent.remove(links[i+j])
                    j += 1
                i += range_len

            # For shorter ranges, separate each from its left neighbor.
            else:
                while range_len > 0:
                    if i > 0:
                        links[i-1].tail = ","
                    i += 1
                    range_len -= 1

        # Add closing bracket, preserving the last node's tail text.
        tail = links[-1].tail
        if tail is None:
            links[-1].tail = "]"
        else:
            links[-1].tail = f"]{tail}"


class DIS(Summary):
    """Drug Information Summary"""

    TYPE = "dis"
    TRANSFORM = etree.XSLT(etree.parse("cms-dis.xsl"))

    @property
    def values(self):
        """Get the pieces of the summary needed by the Drupal CMS

        Don't cache this property, so we avoid accumulating excessive
        memory usage.
        """

        # Tease out the pronunciation fields. Strange that we have one pro-
        # nunciation key, but multiple audio pronunciation clips.
        root = etree.parse(self.path).getroot()
        meta = root.find("DrugInfoMetaData")
        audio_id = None
        pron = meta.find("PronunciationInfo")
        if pron is not None:
            for node in pron.findall("MediaLink"):
                if node.get("language") == "en":
                    ref = node.get("ref")
                    if ref:
                        try:
                            audio_id = self.extract_id(ref)
                            break
                        except Exception:
                            msg = f"CDR{self.id}: invalid audio ID {ref!r}"
                            raise Exception(msg)
            pron = self.get_text(pron.find("TermPronunciation"))

        # Pull everything together.
        prefix = "https://www.cancer.gov"
        description = self.get_text(meta.find("DrugInfoDescription"))
        if len(description) > self.DESCRIPTION_MAX:
            self.logger.warning("Truncating description %r", description)
            description = description[:self.DESCRIPTION_MAX]
        return {
            "cdr_id": self.id,
            "title": self.get_text(root.find("DrugInfoTitle")),
            "description": description,
            "url": meta.find("DrugInfoURL").get("xref").replace(prefix, ""),
            "posted_date": self.get_text(root.find("DateFirstPublished")),
            "updated_date": self.get_text(root.find("DateLastModified")),
            "pron": pron,
            "audio_id": audio_id,
            "body": html.tostring(self.TRANSFORM(root)).decode("utf-8"),
            "type": "pdq_drug_information_summary",
        }


class DrupalClient:
    """
    Client end of the PDQ RESTful APIs in the Drupal CMS

    Class constants:
        NAX_RETRIES - number of times to try again for failures
        BATCH_SIZE - maximum number of documents we can set to `published`
                     in a single chunk
        PRUNE_BATCH_SIZE - maximum number of nodes to process at one time
                           when clearing out older node revisions
        ORPHAN_BATCH_SIZE - how many summary section orphan deletions to
                            request at a time
        URI_PATH - used for routing of PDQ RESTful API requests
        TYPES - names used for the types of PDQ documents we publish
    """

    MAX_RETRIES = 5
    BATCH_SIZE = 25
    PRUNE_BATCH_SIZE = 10
    ORPHAN_BATCH_SIZE = 1000
    URI_PATH = "/pdq/api"
    TYPES = {
        "Summary": ("pdq_cancer_information_summary", "cis"),
        "DrugInformationSummary": ("pdq_drug_information_summary", "dis"),
    }

    def __init__(self, control):
        """
        Required positional argument:
          control - provides access to processing information
        """

        self.control = control
        self.logger.info("DrupalClient created for %s", self.base)

    @cached_property
    def auth(self):
        """Basic authorization credentials pair"""
        return self.control.auth

    @cached_property
    def base(self):
        """Front portion of the PDQ API URL"""
        if not self.control.opts.base:
            raise Exception("base URL is required for pushing summaries")
        return self.control.opts.base

    @cached_property
    def batch_size(self):
        """The number of documents to be marked `published` at once"""
        return self.control.opts.batch or self.BATCH_SIZE

    @cached_property
    def logger(self):
        """Object for recording what we do"""
        return self.control.logger

    @cached_property
    def types(self):
        """Mapping from Drupal class for content to API URL tail"""
        return dict(self.TYPES.values())

    def push(self, values):
        """Send a PDQ document to the Drupal CMS

        The document will be stored in the `draft` state, and must be
        released to the `published` state at the end of the job in batch
        with the other PDQ documents published by the job (see the
        `publish()` method).

        Pass:
          values - dictionary of field values keyed by field name

        Return:
          integer for the ID of the node in which the document is stored
        """

        # Make sure we use the existing node if already in the CMS.
        self.__check_nid(values)

        # Different types use different API URLs.
        t = values["type"]
        url = f"{self.base}{self.URI_PATH}/{self.types[t]}?_format=json"
        self.logger.debug("URL for push(): %s", url)

        # Send the values to the CMS and check for success.
        # TODO: Get Acquia to fix their broken certificates.
        opts = {"json": values, "auth": self.auth, "verify": False}
        tries = self.MAX_RETRIES
        while tries > 0:
            response = post(url, **opts)
            if response.ok:
                break
            tries -= 1
            if tries <= 0:
                self.logger.error("%r failed: %s", url, response.reason)
                raise Exception(response.reason)
            sleep(1)
            args = values["cdr_id"], response.reason
            self.logger.warning("%s: %s (trying again)", *args)

        # Give the caller the node ID where the document was stored.
        parsed = json_loads(response.text)
        nid = int(parsed["nid"])
        args = values["cdr_id"], self.base, nid
        self.logger.debug("Pushed CDR%d to %s as node %d", *args)
        return nid

    def publish(self, documents, **opts):
        """Ask the CMS to set the specified documents to the `published` state.

        We have to break the batch into chunks small enough that memory
        usage will not be an issue.

        Required positional argument:
          documents - sequence of tuples for the PDQ documents which should
                      be switched from `draft` to `published` state, each
                      tuple containing:
                          - integer for the document's unique CDR ID
                          - integer for the Drupal node for the document
                          - language code ('en' or 'es')
                      for example:
                          [
                              (257994, 231, "en"),
                              (257995, 241, "en"),
                              (448617, 226, "es"),
                              (742114, 136, "en"),
                          ]

        Optional keyword argument:
          cleanup - if `True` (the default), invoke the services to drop
                    older revisions of the nodes being published, as well
                    as summary section entities which have no parent
                    summary nodes

        Return:
          possibly empty dictionary of error messages, indexed by the
          CDR ID for documents which failed
        """

        url = f"{self.base}{self.URI_PATH}?_format=json"
        self.logger.info("Marking %d documents published", len(documents))
        self.logger.debug("URL for publish(): %s", url)
        offset = 0
        lookup = {(doc[1:], doc[0]) for doc in documents}
        errors = {}
        while offset < len(documents):
            end = offset + self.batch_size
            chunk = [doc[1:] for doc in documents[offset:end]]
            self.logger.debug("Marking %d docs as published", len(chunk))
            self.logger.debug("Docs: %r", chunk)
            offset = end
            # TODO: Get Acquia to fix their broken certificates.
            opts = {"json": chunk, "auth": self.auth, "verify": False}
            tries = self.MAX_RETRIES
            while tries > 0:
                response = post(url, **opts)
                if not response.ok:
                    tries -= 1
                    if tries <= 0:
                        for key in chunk:
                            cdr_id = lookup[key]
                            errors[cdr_id] = response.reason
                            args = cdr_id, response.reason
                            self.logger.error("CDR%d: %s", *args)
                    else:
                        sleep(1)
                        msg = "publish(): %s (trying again)"
                        self.logger.warning(msg, response.reason)
                else:
                    for nid, lang, err in json_loads(response.text)["errors"]:
                        key = nid, lang
                        cdr_id = lookup[(nid, lang)]
                        errors[cdr_id] = err
                        self.logger.error("CDR%d: %s", cdr_id, err)
                    break
        if opts.get("cleanup", True):
            nodes = sorted({doc[1] for doc in documents})
            self.prune_revisions(nodes)
            self.drop_orphans()
        self.logger.info("%d errors found marking docs published", len(errors))
        return errors

    def lookup(self, cdr_id):
        """Fetch the Drupal ID for document's node (if it exists)

        Pass:
          cdr_id - integer for PDQ document

        Return:
          integer for unique Drupal node ID or None
        """

        url = f"{self.base}{self.URI_PATH}/{cdr_id}?_format=json"
        self.logger.debug("URL for get_nid(): %s", url)
        # TODO: Get Acquia to fix their broken certificates.
        response = get(url, auth=self.auth, verify=False)
        if response.ok:
            parsed = json_loads(response.text)
            if not parsed:
                raise Exception(f"CDR ID {cdr_id} not found")
            if cdr_id > 0 and len(parsed) > 1:
                raise Exception(f"Ambiguous CDR ID {cdr_id}")
            return int(parsed[0][0])
        if response.status_code == 404:
            return None
        code = response.status_code
        reason = response.reason
        raise Exception(f"lookup returned code {code}: {reason}")

    def prune_revisions(self, nodes):
        """
        Ask the CMS to remove older revisions for the published nodes.

        Pass:
          nodes - ordered sequence of IDs for the published nodes
        """

        url = f"{self.base}{self.URI_PATH}/prune"
        self.logger.info("pruning revisions with %r", url)
        offset = 0
        while offset < len(nodes):
            node_ids = nodes[offset:offset+self.PRUNE_BATCH_SIZE]
            offset += self.PRUNE_BATCH_SIZE
            data = {"nodes": node_ids, "keep": 3}
            opts = {"json": data, "auth": self.auth, "verify": False}
            tries = self.MAX_RETRIES
            while tries > 0:
                tries -= 1
                response = patch(url, **opts)
                if response.ok:
                    message = "dropped revisions %s for node %s"
                    for nid, vids in json_loads(response.text):
                        self.logger.debug(message, vids, nid)
                    break
                if tries:
                    message = "prune_revisions(): %s (trying again)"
                    self.logger.warning(message, response.reason)
                    sleep(1)
                else:
                    self.logger.error("prune_revisions: %s", response.reason)

    def drop_orphans(self):
        """Ask the CMS to remove summary sections without parent CIS nodes """

        url = f"{self.base}{self.URI_PATH}/cis/prune"
        self.logger.info("dropping orphans with %r", url)
        opts = {"json": self.ORPHAN_BATCH_SIZE, "auth": self.auth}
        # TODO: Get Acquia to fix their broken certificates.
        opts["verify"] = False
        message = "dropped revisions %s for summary section %s"
        while True:
            response = patch(url, **opts)
            if response.ok:
                dropped = json_loads(response.text)
                if not dropped:
                    break
                for pid, vids in dropped:
                    self.logger.debug(message, vids, pid)
            else:
                self.logger.error("drop_orphans(): %s", response.reason)
                break

    def __check_nid(self, values):
        """Insert node ID for document already in the Drupal CMS

        Node must already exist when storing the Spanish translation
        of the summary (business rule confirmed by Bryan Pizillo).

        Pass:
          values - dictionary of values for the document being stored
                   (we save the node ID here if appropriate as a side
                   effect)
        """

        cdr_id = int(values["cdr_id"])
        if cdr_id > 0 and not values.get("nid"):
            translation_of = values.get("translation_of")
            if translation_of:
                nid = self.lookup(translation_of)
                if not nid:
                    msg = f"CDR{cdr_id}: English summary must be saved first"
                    self.logger.error(msg)
                    raise Exception(msg)
            else:
                nid = self.lookup(values["cdr_id"])
            values["nid"] = nid
        if "nid" not in values:
            values["nid"] = None


if __name__ == "__main__":
    Control().run()
