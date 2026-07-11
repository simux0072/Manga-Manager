import {expect,test} from '@playwright/test'

const pages=['discovery','library','updates','matches','activity','operations']

for(const path of pages){
  test(`${path} is responsive and has no page overflow`,async({page},testInfo)=>{
    await page.goto(`/${path}`)
    await expect(page.locator('main')).toBeVisible()
    const overflow=await page.evaluate(()=>document.documentElement.scrollWidth-document.documentElement.clientWidth)
    expect(overflow).toBeLessThanOrEqual(1)
    await expect(page.locator('h1')).toHaveCount(1)
    const unnamedButtons=await page.locator('button').evaluateAll(buttons=>buttons.filter(button=>!button.textContent?.trim()&&!button.getAttribute('aria-label')&&!button.getAttribute('title')).length)
    expect(unnamedButtons).toBe(0)
    await page.screenshot({path:testInfo.outputPath(`${path}.png`),fullPage:true})
  })
}

test('discovery filters immediately and jobs use an overlay drawer',async({page})=>{
  await page.goto('/discovery')
  await page.getByLabel('Search catalog').fill('painter')
  await expect(page).toHaveURL(/q=painter/)
  await page.getByRole('button',{name:/asura/i}).click()
  await page.getByRole('button',{name:/mangafire/i}).click()
  await expect(page).toHaveURL(/source=asura/)
  await expect(page).toHaveURL(/source=mangafire/)
  const widthBefore=await page.locator('main').evaluate(node=>node.getBoundingClientRect().width)
  await page.getByRole('button',{name:/open job center/i}).click()
  await expect(page.getByRole('complementary',{name:'Job center'})).toBeVisible()
  const widthAfter=await page.locator('main').evaluate(node=>node.getBoundingClientRect().width)
  expect(widthAfter).toBe(widthBefore)
})
